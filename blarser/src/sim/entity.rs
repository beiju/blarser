use std::fmt::{Debug, Display};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl TimedEventType {
    pub fn description(&self) -> String {
        match self {
            TimedEventType::EarlseasonStart => {
                "EarlseasonStart".to_string()
            }
            TimedEventType::DayAdvance => {
                "DayAdvance".to_string()
            }
            TimedEventType::EndTopHalf(_) => {
                "EndTopHalf".to_string()
            }
        }
    }
}

pub trait Entity: for<'de> Deserialize<'de> + PartialInformationCompare + Clone + Debug + Display + PartialEq {
    fn name() -> &'static str;
    fn id(&self) -> Uuid;

    fn new(json: serde_json::Value) -> Self::Raw where Self::Raw: Sized {
        serde_json::from_value(json)
            .expect("Error converting entity JSON to entity type")
    }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<TimedEvent>;
    fn time_range_for_update(valid_from: DateTime<Utc>, raw: &Self::Raw) -> (DateTime<Utc>, DateTime<Utc>);
}

pub fn entity_description(entity_type: &str, entity_json: serde_json::Value) -> String {
    entity_dispatch!(entity_type => entity_description_typed(entity_json);
                     other => format!("({})", other))
}

fn entity_description_typed<EntityT: Entity>(entity_json: serde_json::Value) -> String {
    let entity: EntityT = serde_json::from_value(entity_json)
        .expect("Couldn't deserialize entity json");
    entity.to_string()
}

// Helper used in next_timed_event
pub struct EarliestEvent {
    limit: DateTime<Utc>,
    lowest: Option<TimedEvent>,
}

impl EarliestEvent {
    pub fn new(limit: DateTime<Utc>) -> EarliestEvent {
        EarliestEvent { limit, lowest: None }
    }

    pub fn push(&mut self, new_event: TimedEvent) {
        // The = is important
        if new_event.time <= self.limit { return; }

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

    pub fn into_inner(self) -> Option<TimedEvent> {
        self.lowest
    }
}

#[macro_export]
macro_rules! entity_dispatch {
    ($type_var:ident => $func:ident($($args:expr),*); $fallback_pattern:pat => $fallback_arm:expr) => {
        match $type_var {
            "sim" => $func::<crate::sim::Sim>($($args),*),
            "game" => $func::<crate::sim::Game>($($args),*),
            "team" => $func::<crate::sim::Team>($($args),*),
            "player" => $func::<crate::sim::Player>($($args),*),
            "standings" => $func::<crate::sim::Standings>($($args),*),
            "season" => $func::<crate::sim::Season>($($args),*),
            $fallback_pattern => $fallback_arm,
        }
    };
    ($type_var:ident => $func:ident($($args:expr),*).await; $fallback_pattern:pat => $fallback_arm:expr) => {
        match $type_var {
            "sim" => $func::<crate::sim::Sim>($($args),*).await,
            "game" => $func::<crate::sim::Game>($($args),*).await,
            "team" => $func::<crate::sim::Team>($($args),*).await,
            "player" => $func::<crate::sim::Player>($($args),*).await,
            "standings" => $func::<crate::sim::Standings>($($args),*).await,
            "season" => $func::<crate::sim::Season>($($args),*).await,
            $fallback_pattern => $fallback_arm,
        }
    };
    ($type_var:ident => $ns:ident::$func:ident($($args:expr),*); $fallback_pattern:pat => $fallback_arm:expr) => {
        match $type_var {
            "sim" => $ns::$func::<crate::sim::Sim>($($args),*),
            "game" => $ns::$func::<crate::sim::Game>($($args),*),
            "team" => $ns::$func::<crate::sim::Team>($($args),*),
            "player" => $ns::$func::<crate::sim::Player>($($args),*),
            "standings" => $ns::$func::<crate::sim::Standings>($($args),*),
            "season" => $ns::$func::<crate::sim::Season>($($args),*),
            $fallback_pattern => $fallback_arm,
        }
    };
}

pub use entity_dispatch;