use chrono::{Duration, NaiveDateTime};
use diesel::{insert_into, RunQueryDsl};
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

    fetch_initial_state(ingest, start_at_time).await;

    info!("Finished populating initial Chron values")
}

async fn fetch_initial_state(ingest: IngestState, start_at_time: &'static str) {
// This is a blocking API, so have tokio run it in a separate thread
    let pending_inserts = tokio::task::spawn_blocking(move || {
        let mut pending_inserts = Vec::new();
        for entity_type in chronicler::ENDPOINT_NAMES {
            for entity in chronicler::entities(entity_type, start_at_time) {
                pending_inserts.push(InsertChronUpdate::from_chron(
                    ingest.ingest_id, entity_type, entity, true)
                )
            }
        }
        pending_inserts
    }).await.expect("Failed to fetch initial state from chron");

    ingest.db.run(|c| {
        use crate::schema::chron_updates::dsl::*;

        insert_into(chron_updates).values(pending_inserts).execute(c)
    }).await.expect("Failed to store initial state from chron");

    tokio::time::sleep(core::time::Duration::from_secs(60*60)).await;
}