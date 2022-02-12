use diesel::{Connection, insert_into, Insertable, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::info;
use rocket::warn;

use crate::api::ChroniclerItem;
use crate::db::BlarserDbConn;

use crate::schema::*;
use crate::sim;

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
struct NewVersion {
    ingest_id: i32,
    entity_type: &'static str,
    entity_id: Uuid,
    generation: i32,
    single_parent: Option<i32>,
    start_time: DateTime<Utc>,
    data: serde_json::Value,
    event_type: EventType,
    feed_event_id: Option<Uuid>,
    next_timed_event: Option<DateTime<Utc>>,
}

impl NewVersion {
    fn for_initial_state(ingest_id: i32, start_time: DateTime<Utc>, entity_type: &str, item: ChroniclerItem) -> Option<NewVersion> {
        let version = match entity_type {
            "sim" => Self::for_initial_state_typed::<sim::Sim>(ingest_id, start_time, item),
            "game" => Self::for_initial_state_typed::<sim::Game>(ingest_id, start_time, item),
            "team" => Self::for_initial_state_typed::<sim::Team>(ingest_id, start_time, item),
            "player" => Self::for_initial_state_typed::<sim::Player>(ingest_id, start_time, item),
            _ => {
                // TODO Remove this once all entity types are implemented
                return None
            }
        };

        Some(version)
    }

    fn for_initial_state_typed<EntityT: sim::Entity>(ingest_id: i32, start_time: DateTime<Utc>, item: ChroniclerItem) -> NewVersion {
        let raw: EntityT::Raw = serde_json::from_value(item.data)
            .expect("Couldn't deserialize entity into raw PartialInformation");

        let entity = EntityT::from_raw(raw);
        let next_timed_event = entity.next_timed_event(start_time);

        NewVersion {
            ingest_id,
            entity_type: EntityT::name(),
            entity_id: item.entity_id,
            generation: 0,
            single_parent: None,
            start_time,
            data: serde_json::to_value(entity)
                .expect("Failed to serialize PartialInformation entity"),
            event_type: EventType::Start,
            feed_event_id: None,
            next_timed_event
        }
    }

}

pub async fn add_initial_versions(conn: BlarserDbConn, ingest_id: i32, start_time: DateTime<Utc>,
                                  versions: Vec<(&'static str, ChroniclerItem)>) {
    conn.run(move |c| {
        c.transaction(|| {
            let chunks = versions.into_iter()
                .flat_map(move |(entity_type, item)| {
                    NewVersion::for_initial_state(ingest_id, start_time, entity_type, item)
                })
                .chunks(5000); // Diesel can't handle inserting the whole thing in one go

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
