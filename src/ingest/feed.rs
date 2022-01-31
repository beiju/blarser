use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::{Connection, insert_into, RunQueryDsl};
use rocket::info;
use futures::StreamExt;
use uuid::Uuid;

use crate::api::{eventually, EventuallyEvent};
use crate::ingest::feed_changes;
use crate::ingest::task::IngestState;

use crate::schema::*;

#[derive(Insertable)]
#[table_name = "feed_events"]
struct InsertFeedEvent {
    ingest_id: i32,
    created_at: NaiveDateTime,
    data: serde_json::Value,
}

impl InsertFeedEvent {
    fn from_eventually(ingest_id: i32, item: EventuallyEvent) -> Self {
        InsertFeedEvent {
            ingest_id,
            created_at: item.created.naive_utc(),
            data: serde_json::value::to_value(item)
                .expect("Failed to re-serialize Eventually event"),
        }
    }
}

#[derive(Insertable)]
#[table_name = "feed_event_changes"]
struct InsertFeedEventChange {
    feed_event_id: i32,
    entity_type: &'static str,
    entity_id: Option<Uuid>,
}

impl InsertFeedEventChange {
    fn new(feed_event_id: i32, entity_type: &'static str, entity_id: Option<Uuid>) -> Self {
        InsertFeedEventChange {
            feed_event_id,
            entity_type,
            entity_id,
        }
    }
}


pub async fn ingest_feed(db: IngestState, start_at_time: &'static str) {
    info!("Started Feed ingest task");

    eventually::events(start_at_time)
        .ready_chunks(500)
        .fold(db, |mut ingest, events| async move {
            let processed_up_to = save_feed_events(&ingest, events).await;

            loop {
                let stop_at = *ingest.receive_progress.borrow() + Duration::minutes(1);
                if processed_up_to < stop_at {
                    break;
                }
                info!("Waiting for Chron thread to progress ({}s)", (processed_up_to - stop_at).num_seconds());
                ingest.receive_progress.changed().await
                    .expect("Communication with Chron thread failed");
            }

            ingest.notify_progress.send(processed_up_to)
                .expect("Communication with Chron thread failed");

            ingest
        }).await;
}

async fn save_feed_events(ingest: &IngestState, events: Vec<EventuallyEvent>) -> DateTime<Utc> {
    info!("Got batch of {} events", events.len());

    // TODO Update this to use the header from Eventually somehow
    let last_event_date = events.split_last()
        .expect("save_feed_events was called with no events")
        .0.created;

    let mut insert_events = Vec::new();
    let mut insert_changes = Vec::new();
    for event in events {
        insert_changes.push(feed_changes::changes_for_event(&event));
        insert_events.push(InsertFeedEvent::from_eventually(ingest.ingest_id, event));
    }

    ingest.db.run(|c| {
        c.transaction(|| {
            use crate::schema::feed_events::dsl as feed_events;
            use crate::schema::feed_event_changes::dsl as feed_event_changes;

            let ids: Vec<i32> = insert_into(feed_events::feed_events)
                .values(insert_events)
                .returning(feed_events::id)
                .get_results(c)?;

            let changes: Vec<_> = ids.into_iter()
                .zip(insert_changes.into_iter())
                .map(|(feed_event_id, changes)| {
                    changes.into_iter().map(move |(entity_type, entity_id)| {
                        InsertFeedEventChange::new(feed_event_id, entity_type, entity_id)
                    })
                })
                .flatten()
                .collect();

            insert_into(feed_event_changes::feed_event_changes)
                .values(changes)
                .execute(c)?;

            Ok::<_, diesel::result::Error>(())
        })
    }).await.expect("Failed to insert feed events");

    last_event_date
}