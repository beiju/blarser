use std::collections::HashSet;
use std::rc::Rc;
use thiserror::Error;
use std::fmt::Write;
use serde_json::Value as JsonValue;
use log::debug;

use crate::blaseball_state::{BlaseballState, Event, KnownValue, PropertyValue, Uuid, Value as StateValue, Value};
use crate::ingest::eventually_schema::{EventType, EventuallyEvent};

pub fn apply_feed_event(state: Rc<BlaseballState>, event: EventuallyEvent) -> Rc<BlaseballState> {
    debug!("Applying event: {}", event.description);

    match event.r#type {
        EventType::BigDeal => apply_big_deal(state, event),
        _ => todo!()
    }
}

fn apply_big_deal(state: Rc<BlaseballState>, event: EventuallyEvent) -> Rc<BlaseballState> {
    debug!("Applying BigDeal event");

    Rc::new(BlaseballState {
        predecessor: Some(state.clone()),
        from_event: Rc::new(Event::BigDeal {
            feed_event_id: Uuid::new(event.id)
        }),
        data: state.data.clone(),
    })
}

