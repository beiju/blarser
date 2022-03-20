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

#[derive(Debug, Clone, Serialize, Deserialize)]
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