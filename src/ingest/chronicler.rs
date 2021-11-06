use std::rc::Rc;
use chrono::{DateTime, Utc};
use log::info;

use crate::api::{ChroniclerItem, chronicler};
use crate::blaseball_state::BlaseballState;
use crate::ingest::{IngestItem, IngestError};

pub struct ChronUpdate {
    endpoint: &'static str,
    item: ChroniclerItem,
}

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=Box<dyn IngestItem>>>> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |endpoint|
            Box::new(chronicler::versions(endpoint, start)
                .map(|item| Box::new(ChronUpdate { endpoint, item }) as Box<dyn IngestItem>))
            as Box<dyn Iterator<Item = Box<(dyn IngestItem)>>>
        )
        .collect()
}

impl IngestItem for ChronUpdate {
    fn date(&self) -> DateTime<Utc> {
        self.item.valid_from
    }

    fn apply(self: Box<Self>, state: Rc<BlaseballState>) -> Result<Rc<BlaseballState>, IngestError> {
        info!("Applying chron update for {}", self.endpoint);

        Ok(state)
    }
}