use chrono::{DateTime, Utc};
use diesel::{insert_into, PgConnection, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use itertools::Itertools;
use crate::api::EventuallyEvent;

use crate::schema::*;
use crate::sim::{TimedEvent, TimedEventType};

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
    event_time: DateTime<Utc>,
    event_source: EventSource,
    event_data: serde_json::Value,
}

#[derive(Identifiable, Queryable, PartialEq, Debug)]
#[table_name = "events"]
pub struct Event {
    pub id: i32,
    pub ingest_id: i32,
    pub event_time: DateTime<Utc>,
    pub event_source: EventSource,
    pub event_data: serde_json::Value
}

impl Event {
    pub fn description(self) -> Result<String, serde_json::error::Error> {
        match self.event_source {
            EventSource::Start => { Ok("Start".to_string())}
            EventSource::Feed => {
                let event: EventuallyEvent = serde_json::from_value(self.event_data)?;
                let description = event.metadata.siblings.into_iter()
                    .map(|event| event.description)
                    .join("\n");

                Ok(description)
            }
            EventSource::Timed => {
                let event: TimedEventType = serde_json::from_value(self.event_data)?;
                Ok(event.description())
            }
            EventSource::Manual => {
                todo!()
            }
        }
    }
}

fn insert_event(c: &PgConnection, event: NewEvent) -> diesel::result::QueryResult<i32> {
    use crate::schema::events::dsl as events;

    insert_into(events::events)
        .values(event)
        .returning(events::id)
        .get_result::<i32>(c)
}

pub fn add_start_event(c: &PgConnection, ingest_id: i32, event_time: DateTime<Utc>) -> i32 {
    insert_event(c, NewEvent {
        ingest_id,
        event_time,
        event_source: EventSource::Start,
        event_data: serde_json::Value::Null,
    }).expect("Error inserting start event")
}

pub fn add_timed_event(c: &PgConnection, ingest_id: i32, event: TimedEvent) -> i32 {
    insert_event(c, NewEvent {
        ingest_id,
        event_time: event.time,
        event_source: EventSource::Timed,
        event_data: serde_json::to_value(event.event_type)
            .expect("Error serializing TimedEvent"),
    }).expect("Error inserting timed event")
}

pub fn add_feed_event(c: &PgConnection, ingest_id: i32, event: EventuallyEvent) -> i32 {
    insert_event(c, NewEvent {
        ingest_id,
        event_time: event.created,
        event_source: EventSource::Feed,
        event_data: serde_json::to_value(event)
            .expect("Error serializing EventuallyEvent"),
    }).expect("Error inserting feed event")
}