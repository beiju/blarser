use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use diesel::{insert_into, PgConnection, RunQueryDsl};
use diesel_derive_enum::DbEnum;
use itertools::Itertools;
use rocket::info;
use crate::api::EventuallyEvent;

use crate::schema::*;
use crate::sim::{TimedEvent, TimedEventType};
use crate::state::{ChronObservationEvent, IngestEvent};
use crate::StateInterface;

// define your enum
#[derive(PartialEq, Debug, DbEnum)]
#[DieselType = "Event_source"]
pub enum EventSource {
    Start,
    Feed,
    Timed,
    Chron,
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
    pub event_data: serde_json::Value,
}

pub enum EventData {
    Start,
    Feed(EventuallyEvent),
    Timed(TimedEvent),
    Manual(ChronObservationEvent)
}

impl EventData {
    pub fn apply(&self, state: &impl StateInterface) {
        match self {
            EventData::Start => {
                panic!("Can't re-apply a Start event!")
            }
            EventData::Feed(feed_event) => {
                feed_event.apply(state)
            }
            EventData::Timed(timed_event) => {
                info!("In chronicler, re-applying timed event {:?}", timed_event.event_type);
                timed_event.apply(state)
            }
            EventData::Manual(_) => {
                panic!("Can't re-apply a Manual event!")
            }
        }
    }

    pub fn type_str(&self) -> String {
        match self {
            EventData::Start => { "Start".to_string() }
            EventData::Feed(e) => { format!("{:?}", e.r#type) }
            EventData::Timed(t) => { format!("{:?}", t.event_type) }
            EventData::Manual(_) => { "Manual".to_string() }
        }
    }
}

impl Display for EventData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventData::Start => { write!(f, "Start") }
            EventData::Feed(event) => {
                if event.metadata.siblings.is_empty() {
                    write!(f, "{}", event.description)
                } else {
                    write!(f, "{}", event.metadata.siblings.iter()
                        .map(|event| &event.description)
                        .join("\n"))
                }
            }
            EventData::Timed(event) => {
                write!(f, "{}", event.event_type.description())
            }
            EventData::Manual(event) => {
                write!(f, "{}", event.description())
            }
        }
    }
}

impl Event {
    pub fn parse(self) -> Result<EventData, serde_json::error::Error> {
        match self.event_source {
            EventSource::Start => { Ok(EventData::Start) }
            EventSource::Feed => {
                let event: EventuallyEvent = serde_json::from_value(self.event_data)?;
                Ok(EventData::Feed(event))
            }
            EventSource::Timed => {
                let event: TimedEventType = serde_json::from_value(self.event_data)?;
                Ok(EventData::Timed(TimedEvent {
                    time: self.event_time,
                    event_type: event
                }))
            }
            EventSource::Chron => {
                let event: ChronObservationEvent = serde_json::from_value(self.event_data)?;
                Ok(EventData::Manual(event))
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
        event_time: event_time,
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

pub fn add_chron_event(c: &PgConnection, ingest_id: i32, event: ChronObservationEvent) -> i32 {
    insert_event(c, NewEvent {
        ingest_id,
        event_time: event.applied_at,
        event_source: EventSource::Chron,
        event_data: serde_json::to_value(event)
            .expect("Error serializing ChronObservationEvent"),
    }).expect("Error inserting chron event")
}