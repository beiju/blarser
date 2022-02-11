use diesel::{Connection, insert_into, Insertable, PgConnection, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use crate::api::ChroniclerItem;
use crate::db::BlarserDbConn;

use crate::schema::*;

// define your enum
#[derive(Debug, DbEnum)]
#[DieselType = "Event_type"]
pub enum EventType {
    Start,
    Feed,
    Manual,
    // timed
    EarlseasonStart,
    DayStart,
}


#[derive(Insertable)]
#[table_name = "versions"]
pub struct NewVersion {
    ingest_id: i32,
    entity_type: &'static str,
    entity_id: Uuid,
    generation: i32,
    single_parent: Option<i32>,
    start_time: DateTime<Utc>,
    data: serde_json::Value,
    event_type: EventType,
    feed_event_id: Option<Uuid>,
}

pub async fn add_initial_versions(conn: BlarserDbConn, ingest_id: i32, start_time: DateTime<Utc>,
                                  versions: Vec<(&'static str, ChroniclerItem)>) {
    conn.run(move |c| {
        c.transaction(|| {
            let mut chunks = versions.into_iter()
                .map(move |(entity_type, item)| {
                    NewVersion {
                        ingest_id,
                        entity_type,
                        entity_id: item.entity_id,
                        generation: 0,
                        single_parent: None,
                        start_time,
                        data: item.data,
                        event_type: EventType::Start,
                        feed_event_id: None
                    }
                })
                .chunks(500); // Diesel can't handle inserting the whole thing in one go

            for chunk in &chunks {
                use crate::schema::versions::dsl::*;
                insert_into(versions)
                    .values(chunk.collect::<Vec<_>>())
                    .execute(c)?;
            }

            Ok::<_, diesel::result::Error>(())
        })
    }).await
        .expect("Failed to save initial versions")
}

