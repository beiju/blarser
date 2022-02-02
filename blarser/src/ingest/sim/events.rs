use chrono::{DateTime, Utc};
use crate::api::EventuallyEvent;

#[derive(Debug)]
pub enum EventType {
    FeedEvent(EventuallyEvent),

    // Timed events
    EarlseasonStart,
}

pub struct GenericEvent {
    pub time: DateTime<Utc>,
    pub event_type: EventType,
}

