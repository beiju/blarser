use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;

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

pub trait Entity: for<'de> Deserialize<'de> + PartialInformationCompare + Clone + Debug + PartialEq {
    fn name() -> &'static str;
    fn id(&self) -> Uuid;

    fn new(json: serde_json::Value) -> Self::Raw where Self::Raw: Sized {
        serde_json::from_value(json)
            .expect("Error converting entity JSON to entity type")
    }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<TimedEvent>;
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
