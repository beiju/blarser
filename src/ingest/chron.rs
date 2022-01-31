use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::{Connection, insert_into, RunQueryDsl};
use futures::{stream, Stream, StreamExt};
use rocket::info;
use uuid::Uuid;
use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;

use crate::schema::*;

#[derive(Insertable)]
#[table_name = "chron_updates"]
struct InsertChronUpdate {
    ingest_id: i32,
    entity_type: &'static str,
    entity_id: Uuid,
    perceived_at: NaiveDateTime,
    earliest_time: NaiveDateTime,
    latest_time: NaiveDateTime,
    resolved: bool,
    data: serde_json::Value,
}

impl InsertChronUpdate {
    fn from_chron(ingest_id: i32, entity_type: &'static str, item: ChroniclerItem, resolved: bool) -> Self {
        InsertChronUpdate {
            ingest_id,
            entity_type,
            entity_id: item.entity_id,
            perceived_at: item.valid_from.naive_utc(),
            earliest_time: (item.valid_from - Duration::seconds(5)).naive_utc(),
            latest_time: (item.valid_from + Duration::seconds(5)).naive_utc(),
            resolved,
            data: item.data,
        }
    }
}

async fn fetch_initial_state(ingest: IngestState, start_at_time: &'static str) -> IngestState {
    let ingest_id = ingest.ingest_id;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::entities(entity_type, start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest_id, entity_type, entity, true)
                });

            Box::pin(stream) as Pin<Box<dyn Stream<Item=InsertChronUpdate> + Send>>
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest_id, "game", entity, true)
                })
            ) as Pin<Box<dyn Stream<Item=InsertChronUpdate> + Send>>
        ));

    // There are so many objects that Diesel can't insert them all in one operation
    let inserts_chunked: Vec<_> = stream::select_all(streams)
        .chunks(1000)
        .collect().await;

    ingest.db.run(|c| {
        c.transaction(|| {
            use crate::schema::chron_updates::dsl::*;

            for insert_chunk in inserts_chunked {
                insert_into(chron_updates).values(insert_chunk).execute(c)?;
            }

            Ok::<_, diesel::result::Error>(())
        })
    }).await.expect("Failed to store initial state from chron");

    ingest
}

// This differs from InsertChronUpdate just on the type of the time fields
struct PendingChronUpdate {
    ingest_id: i32,
    entity_type: &'static str,
    entity_id: Uuid,
    perceived_at: DateTime<Utc>,
    earliest_time: DateTime<Utc>,
    latest_time: DateTime<Utc>,
    resolved: bool,
    data: serde_json::Value,
}

impl PendingChronUpdate {
    fn from_chron(ingest_id: i32, entity_type: &'static str, item: ChroniclerItem, resolved: bool) -> Self {
        PendingChronUpdate {
            ingest_id,
            entity_type,
            entity_id: item.entity_id,
            perceived_at: item.valid_from,
            earliest_time: item.valid_from - Duration::seconds(5),
            latest_time: item.valid_from + Duration::seconds(5),
            resolved,
            data: item.data,
        }
    }
}

pub async fn ingest_chron(ingest: IngestState, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    // Have to move ingest in and back out even though that's the whole point of borrows
    let ingest = fetch_initial_state(ingest, start_at_time).await;

    info!("Finished populating initial Chron values");

    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                .map(move |entity| {
                    PendingChronUpdate::from_chron(ingest.ingest_id, entity_type, entity, true)
                });

            Box::pin(stream) as Pin<Box<dyn Stream<Item=PendingChronUpdate> + Send>>
        })
        .chain(iter::once(
            Box::pin(chronicler::game_updates(start_at_time)
                .map(move |entity| {
                    PendingChronUpdate::from_chron(ingest.ingest_id, "game", entity, true)
                })
            ) as Pin<Box<dyn Stream<Item=PendingChronUpdate> + Send>>
        ));

    stream::select_all(streams)
        .fold(ingest, |mut ingest, update| async {
            wait_for_feed_ingest(&mut ingest, update.latest_time).await;

            let update_time = do_ingest(&mut ingest, update).await;

            ingest.notify_progress.send(update_time)
                .expect("Error communicating with Eventually ingest");

            ingest
        }).await;
}

async fn wait_for_feed_ingest(ingest: &mut IngestState, wait_until_time: DateTime<Utc>) {
    loop {
        let feed_time = *ingest.receive_progress.borrow();
        if feed_time < wait_until_time {
            break;
        }
        info!("Chronicler ingest waiting for Eventually ingest to catch up ({}s)",
            (wait_until_time - feed_time).num_seconds());
        ingest.receive_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}

async fn do_ingest(ingest: &mut IngestState, update: PendingChronUpdate) -> DateTime<Utc> {
    info!("Doing ingest");
    update.latest_time
}