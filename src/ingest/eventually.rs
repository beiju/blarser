use std::sync::Arc;
use chrono::{DateTime, Utc};
use log::debug;
use rocket::async_trait;

use crate::api::{eventually, EventuallyEvent, EventType};
use crate::blaseball_state as bs;
use crate::ingest::IngestItem;
use crate::ingest::error::IngestError;

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=Box<dyn IngestItem + Send>> + Send>> {
    vec![
        Box::new(eventually::events(start)
            .map(|event| Box::new(event) as Box<dyn IngestItem + Send>))
    ]
}

#[async_trait]
impl IngestItem for EventuallyEvent {
    fn date(&self) -> DateTime<Utc> {
        self.created
    }

    async fn apply(self: Box<Self>, state: Arc<bs::BlaseballState>) -> Result<Arc<bs::BlaseballState>, IngestError> {
        Ok(apply_feed_event(state, self))
    }
}

pub fn apply_feed_event(state: Arc<bs::BlaseballState>, event: Box<EventuallyEvent>) -> Arc<bs::BlaseballState> {
    debug!("Applying Feed event: {}", event.description);

    match event.r#type {
        EventType::BigDeal => apply_big_deal(state, event),
        _ => todo!()
    }
}

fn apply_big_deal(state: Arc<bs::BlaseballState>, _: Box<EventuallyEvent>) -> Arc<bs::BlaseballState> {
    debug!("Ignoring BigDeal event");
    state
}
