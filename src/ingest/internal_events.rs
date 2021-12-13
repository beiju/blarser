use std::sync::Arc;
use chrono::{DateTime, Utc};
use rocket::async_trait;

use crate::blaseball_state::BlaseballState;
use crate::ingest::IngestItem;
use crate::ingest::error::IngestApplyResult;
use crate::ingest::log::IngestLogger;

pub struct StartSeasonItem {
    at_time: DateTime<Utc>,
}

impl StartSeasonItem {
    pub fn new(at_time: DateTime<Utc>) -> StartSeasonItem {
        StartSeasonItem {
            at_time
        }
    }
}

#[async_trait]
impl IngestItem for StartSeasonItem {
    fn date(&self) -> DateTime<Utc> {
        self.at_time
    }

    async fn apply(&self, log: &IngestLogger, state: Arc<BlaseballState>) -> IngestApplyResult {
        todo!()
    }
}