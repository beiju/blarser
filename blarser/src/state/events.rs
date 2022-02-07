use chrono::{DateTime, Utc};
use crate::api::EventuallyEvent;

#[derive(Debug)]
pub enum GenericEventType {
    FeedEvent(EventuallyEvent),

    // Timed events
    EarlseasonStart,
    DayAdvance,

    // There's a game update without a feed event, so here's an event for it.
    EndTopHalf,
}

#[derive(Debug)]
pub struct GenericEvent {
    pub time: DateTime<Utc>,
    pub event_type: GenericEventType,
}

