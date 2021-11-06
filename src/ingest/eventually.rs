use std::rc::Rc;
use chrono::{DateTime, Utc};
use log::info;

use crate::api::{eventually, EventuallyEvent};
use crate::blaseball_state::BlaseballState;
use crate::ingest::IngestItem;
use crate::ingest::source::IngestError;

pub fn sources(start: &'static str) -> impl Iterator<Item=Box<impl IngestItem>> {
    eventually::events(start)
        .map(|event| Box::new(event))
}

impl IngestItem for EventuallyEvent {
    fn date(&self) -> DateTime<Utc> {
        self.created
    }

    fn apply(&self, state: Rc<BlaseballState>) -> Result<Rc<BlaseballState>, IngestError> {
        info!("Applying {}", self.description);
        Ok(state)
    }
}