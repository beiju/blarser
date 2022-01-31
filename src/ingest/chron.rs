use chrono::{Duration, NaiveDateTime};
use diesel::{insert_into, RunQueryDsl};
use futures::{stream, StreamExt};
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

pub async fn ingest_chron(ingest: IngestState, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    // Have to move ingest in and back out even though that's the whole point of borrows
    let ingest = fetch_initial_state(ingest, start_at_time).await;

    info!("Finished populating initial Chron values");

    loop {
        tokio::time::sleep(core::time::Duration::from_secs(5)).await;
        info!("Pretending to advance chron 1 minute");
        let t = *ingest.notify_progress.borrow() + Duration::minutes(1);
        info!("Got t");
        ingest.notify_progress.send(t)
            .expect("Communication with Eventually thread failed");
        info!("Sent t");
    }
}

async fn fetch_initial_state(ingest: IngestState, start_at_time: &'static str) -> IngestState {
    let ingest_id = ingest.ingest_id;
    let inserts: Vec<_> = stream::iter(chronicler::ENDPOINT_NAMES.into_iter())
        .map(move |entity_type| {
            chronicler::entities(entity_type, start_at_time.clone())
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest_id, entity_type, entity, true)
                })
        })
        .flatten()
        .collect().await;

    ingest.db.run(|c| {
        use crate::schema::chron_updates::dsl::*;

        insert_into(chron_updates).values(inserts).execute(c)
    }).await.expect("Failed to store initial state from chron");

    ingest
}