use std::fmt::{Debug, Display};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use crate::sim;

#[derive(Debug, Clone, Serialize)]
pub enum TimedEventType {
    EarlseasonStart,
    DayAdvance,
    EndTopHalf(Uuid),
}

#[derive(Debug, Clone)]
pub struct TimedEvent {
    pub time: DateTime<Utc>,
    pub event_type: TimedEventType,
}

pub trait Entity: for<'de> Deserialize<'de> + PartialInformationCompare + Clone + Debug + Display + PartialEq {
    fn name() -> &'static str;
    fn id(&self) -> Uuid;

    fn new(json: serde_json::Value) -> Self::Raw where Self::Raw: Sized {
        serde_json::from_value(json)
            .expect("Error converting entity JSON to entity type")
    }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<TimedEvent>;
}

pub fn entity_description(entity_type: &str, entity_json: serde_json::Value) -> String {
    match entity_type {
        "sim" => entity_description_typed::<sim::Sim>(entity_json),
        "game" => entity_description_typed::<sim::Game>(entity_json),
        "player" => entity_description_typed::<sim::Player>(entity_json),
        "team" => entity_description_typed::<sim::Team>(entity_json),
        other => format!("({})", other),
    }
}

fn entity_description_typed<EntityT: Entity>(entity_json: serde_json::Value) -> String {
    let entity: EntityT = serde_json::from_value(entity_json)
        .expect("Couldn't deserialize entity json");
    entity.to_string()
}

// Helper used in next_timed_event
pub struct EarliestEvent {
    limit: DateTime<Utc>,
    lowest: Option<TimedEvent>
}

impl EarliestEvent {
    pub fn new(limit: DateTime<Utc>) -> EarliestEvent {
        EarliestEvent { limit, lowest: None }
    }

    pub fn push(&mut self, new_event: TimedEvent) {
        // The = is important
        if new_event.time <= self.limit { return }

        match &self.lowest {
            None => {
                self.lowest = Some(new_event)
            }
            Some(prev_event) if &new_event.time < &prev_event.time => {
                self.lowest = Some(new_event)
            }
            _ => {}
        }
    }

    pub fn push_opt(&mut self, opt: Option<TimedEvent>) {
        if let Some(event) = opt {
            self.push(event);
        }
    }

    pub fn into_inner(self) -> Option<TimedEvent> {
        self.lowest
    }
}
