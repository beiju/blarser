use std::sync::Arc;
use rocket::async_trait;
use chrono::{DateTime, Utc};

use crate::api::{eventually, EventuallyEvent, EventType};
use crate::blaseball_state as bs;
use crate::ingest::IngestItem;
use crate::ingest::error::IngestResult;
use crate::ingest::log::IngestLogger;

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

    async fn apply(self: Box<Self>, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestResult {
        apply_feed_event(state, log, self).await
    }
}

pub async fn apply_feed_event(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: Box<EventuallyEvent>) -> IngestResult {
    log.debug(format!("Applying Feed event: {}", event.description)).await?;

    match event.r#type {
        EventType::BigDeal => apply_big_deal(state, log, event).await,
        _ => todo!()
    }
}

async fn apply_big_deal(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Ignoring BigDeal event".to_string()).await?;
    Ok(state)
}
