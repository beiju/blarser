use chrono::{DateTime, Utc};
use diesel_derive_enum::DbEnum;
use uuid::Uuid;

use crate::schema::*;
use crate::state::EntityType;
// use crate::events::AnyEvent;

#[derive(PartialEq, Debug, DbEnum, Clone)]
#[DieselTypePath = "crate::schema::sql_types::EventSource"]
pub enum EventSource {
    Start,
    Feed,
    Timed,
    Manual,
}

#[derive(Insertable)]
#[diesel(table_name = events)]
pub(crate) struct NewEvent {
    pub(crate) ingest_id: i32,
    pub(crate) time: DateTime<Utc>,
    pub(crate) source: EventSource,
    pub(crate) data: serde_json::Value,
}

#[derive(Identifiable, Queryable, PartialEq, Debug, Clone)]
#[diesel(table_name = events)]
pub(crate) struct DbEvent {
    pub id: i32,
    pub ingest_id: i32,

    pub time: DateTime<Utc>,
    pub source: EventSource,
    pub data: serde_json::Value,
}

#[derive(Insertable)]
#[diesel(table_name = event_effects)]
pub(crate) struct NewEventEffect {
    pub(crate) event_id: i32,
    pub(crate) entity_type: EntityType,
    pub(crate) entity_id: Option<Uuid>,
    pub(crate) aux_data: serde_json::Value,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[diesel(belongs_to(DbEvent, foreign_key = event_id))]
#[diesel(table_name = event_effects)]
pub struct EventEffect {
    pub id: i32,
    pub event_id: i32,

    pub entity_type: EntityType,
    pub entity_id: Option<Uuid>,
    pub aux_data: serde_json::Value,
}

// pub struct StoredEvent {
//     pub id: i32,
//     pub ingest_id: i32,
//     pub time: DateTime<Utc>,
//     pub source: EventSource,
//     pub event: AnyEvent,
// }

// impl DbEvent {
//     pub fn parse(self) -> StoredEvent {
//         StoredEvent {
//             id: self.id,
//             ingest_id: self.ingest_id,
//             time: self.time,
//             source: self.source,
//             event: serde_json::from_value(self.data)
//                 .expect("Failed to parse event from database")
//         }
//     }
// }

