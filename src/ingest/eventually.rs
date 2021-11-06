use std::rc::Rc;
use chrono::{DateTime, Utc};
use log::debug;

use crate::api::{eventually, EventuallyEvent, EventType};
use crate::blaseball_state as bs;
use crate::ingest::IngestItem;
use crate::ingest::error::IngestError;

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=Box<dyn IngestItem>>>> {
    vec![
        Box::new(eventually::events(start)
            .map(|event| Box::new(event) as Box<dyn IngestItem>))
    ]
}

impl IngestItem for EventuallyEvent {
    fn date(&self) -> DateTime<Utc> {
        self.created
    }

    fn apply(self: Box<Self>, state: Rc<bs::BlaseballState>) -> Result<Rc<bs::BlaseballState>, IngestError> {
        Ok(apply_feed_event(state, self))
    }
}

pub fn apply_feed_event(state: Rc<bs::BlaseballState>, event: Box<EventuallyEvent>) -> Rc<bs::BlaseballState> {
    debug!("Applying Feed event: {}", event.description);

    match event.r#type {
        EventType::BigDeal => apply_big_deal(state, event),
        _ => todo!()
    }
}

fn apply_big_deal(state: Rc<bs::BlaseballState>, _: Box<EventuallyEvent>) -> Rc<bs::BlaseballState> {
    debug!("Ignoring BigDeal event");
    state
}
