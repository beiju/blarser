use chrono::{DateTime, Utc};
use diesel::{insert_into, PgConnection, QueryResult, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use diesel::prelude::*;
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::schema::*;
use crate::entity::TimedEvent;
use crate::events::Event;
use crate::ingest::ChronObservationEvent;

// define your enum
#[derive(PartialEq, Debug, DbEnum)]
#[DieselType = "Event_source"]
pub enum EventSource {
    Start,
    Feed,
    Timed,
    Manual,
}

#[derive(Insertable)]
#[table_name = "events"]
struct NewEvent {
    ingest_id: i32,
    time: DateTime<Utc>,
    source: EventSource,
    data: serde_json::Value,
}

#[derive(Identifiable, Queryable, PartialEq, Debug)]
#[table_name = "events"]
pub(crate) struct DbEvent {
    pub id: i32,
    pub ingest_id: i32,
    pub time: DateTime<Utc>,
    pub source: EventSource,
    pub data: serde_json::Value,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[belongs_to(parent = "DbEvent", foreign_key = "event_id")]
#[table_name = "event_effects"]
pub(crate) struct EventEffect {
    pub id: i32,
    pub event_id: i32,

    pub entity_type: String,
    pub entity_id: Uuid,
    pub aux_data: serde_json::Value,
}

pub struct StoredEvent {
    pub id: i32,
    pub ingest_id: i32,
    pub time: DateTime<Utc>,
    pub source: EventSource,
    pub event: Event,
}

impl DbEvent {
    pub fn parse(self) -> Result<StoredEvent, serde_json::error::Error> {
        serde_json::from_value(self.event_data)
            .map(|event| {
                StoredEvent {
                    id: self.id,
                    ingest_id: self.ingest_id,
                    time: self.time,
                    source: self.source,
                    event
                }
            })
    }
}

fn insert_event(c: &PgConnection, event: NewEvent) -> diesel::result::QueryResult<i32> {
    use crate::schema::events::dsl as events;

    insert_into(events::events)
        .values(event)
        .returning(events::id)
        .get_result::<i32>(c)
}

pub fn insert_events(c: &PgConnection, event: Vec<NewEvent>) -> diesel::result::QueryResult<usize> {
    use crate::schema::events::dsl as events;

    insert_into(events::events)
        .values(event)
        .execute(c)
}

pub fn add_start_event(c: &PgConnection, ingest_id: i32, event_time: DateTime<Utc>) -> QueryResult<i32> {
    insert_event(c, NewEvent {
        ingest_id,
        time: event_time,
        source: EventSource::Start,
        data: serde_json::Value::Null,
    })
}

pub fn add_timed_event(c: &PgConnection, ingest_id: i32, event: TimedEvent) -> QueryResult<i32> {
    insert_event(c, NewEvent {
        ingest_id,
        time: event.time,
        source: EventSource::Timed,
        data: serde_json::to_value(event.event_type)
            .expect("Error serializing TimedEvent"),
    })
}

pub fn add_feed_event(c: &PgConnection, ingest_id: i32, event: EventuallyEvent) -> QueryResult<i32> {
    insert_event(c, NewEvent {
        ingest_id,
        time: event.created,
        source: EventSource::Feed,
        data: serde_json::to_value(event)
            .expect("Error serializing EventuallyEvent"),
    })
}

pub fn add_chron_event(c: &PgConnection, ingest_id: i32, event: ChronObservationEvent) -> QueryResult<i32> {
    insert_event(c, NewEvent {
        ingest_id,
        time: event.applied_at,
        source: EventSource::Manual,
        data: serde_json::to_value(event)
            .expect("Error serializing ChronObservationEvent"),
    })
}