use chrono::{DateTime, Duration, Utc};
use diesel::{Connection, PgConnection};
use rocket::info;
use futures::{pin_mut, StreamExt};

use crate::api::{eventually, EventuallyEvent};
use crate::ingest::task::FeedIngest;

use crate::{sim, FeedStateInterface};
use crate::state::{add_feed_event, add_timed_event, get_version_with_next_timed_event, IngestEvent};

pub async fn ingest_feed(mut ingest: FeedIngest, start_at_time: &'static str) {
    info!("Started Feed ingest task");

    let feed_events = eventually::events(start_at_time);

    pin_mut!(feed_events);

    while let Some(feed_event) = feed_events.next().await {
        let feed_event_time = feed_event.created;
        // Doing a "manual borrow" of ingest because I can't figure out how to please the borrow
        // checker with a proper borrow
        ingest = apply_timed_events_until(ingest, feed_event_time).await;
        ingest = apply_feed_event(ingest, feed_event).await;

        wait_for_chron_ingest(&mut ingest, feed_event_time).await
    }
}

async fn apply_timed_events_until(ingest: FeedIngest, feed_event_time: DateTime<Utc>) -> FeedIngest {
    ingest.db.run(move |c| {
        c.build_transaction()
            .serializable()
            .run(|| {
                while let Some((entity_type, value, entity_time)) = get_version_with_next_timed_event(c, ingest.ingest_id, feed_event_time) {
                    match entity_type.as_str() {
                        "sim" => apply_timed_event::<sim::Sim>(c, ingest.ingest_id, value, entity_time, feed_event_time),
                        "game" => apply_timed_event::<sim::Game>(c, ingest.ingest_id, value, entity_time, feed_event_time),
                        "team" => apply_timed_event::<sim::Team>(c, ingest.ingest_id, value, entity_time, feed_event_time),
                        "player" => apply_timed_event::<sim::Player>(c, ingest.ingest_id, value, entity_time, feed_event_time),
                        &_ => { panic!("Tried to deserialize entity of unknown type {}", entity_type) }
                    }
                }

                Ok::<_, diesel::result::Error>(())
            })
    }).await
        .expect("Database error applying timed events");

    ingest
}

fn apply_timed_event<EntityT: sim::Entity>(c: &PgConnection, ingest_id: i32, value: serde_json::Value, entity_time: DateTime<Utc>, feed_event_time: DateTime<Utc>) {
    let entity: EntityT = serde_json::from_value(value)
        .expect("Error deserializing entity for timed event");

    let event = entity.next_timed_event(entity_time)
        .expect("get_version_with_next_timed_event returned a version without a timed event");
    assert!(event.time > entity_time);
    assert!(event.time <= feed_event_time);

    let from_event = add_timed_event(c, ingest_id, event.clone());

    let state = FeedStateInterface::new(c, ingest_id, from_event, event.time);

    info!("Applying timed event {:?}", event);
    event.apply(&state);
}

async fn apply_feed_event(ingest: FeedIngest, event: EventuallyEvent) -> FeedIngest {
    ingest.db.run(move |c| {
        c.build_transaction()
            .serializable()
            .run(|| {
                let from_event = add_feed_event(c, ingest.ingest_id, event.clone());

                let state = FeedStateInterface::new(c, ingest.ingest_id, from_event, event.created);

                info!("Applying feed event {:?}", event);
                event.apply(&state);

                Ok::<_, diesel::result::Error>(())
            })
    }).await
        .expect("Ingest failed");

    ingest
}

async fn wait_for_chron_ingest(ingest: &mut FeedIngest, feed_event_time: DateTime<Utc>) {
    ingest.send_feed_progress.send(feed_event_time)
        .expect("Error communicating with Chronicler ingest");
    info!("Feed ingest sent progress {}", feed_event_time);

    loop {
        let chron_requests_time = *ingest.receive_chron_progress.borrow();
        let stop_at = chron_requests_time + Duration::seconds(1);
        if feed_event_time < stop_at {
            break;
        }
        info!("Eventually ingest waiting for Chronicler ingest to catch up (at {} and we are at {}, {}s ahead)",
                    chron_requests_time, feed_event_time, (feed_event_time - chron_requests_time).num_seconds());
        ingest.receive_chron_progress.changed().await
            .expect("Error communicating with Chronicler ingest");
    }
}