use std::sync::Arc;
use rocket::async_trait;
use chrono::{DateTime, Utc};

use crate::blaseball_state::BlaseballState;
use crate::ingest::IngestError;
use crate::ingest::log::IngestLogger;

#[async_trait]
pub trait IngestItem {
    fn date(&self) -> DateTime<Utc>;
    async fn apply(self: Box<Self>, log: &IngestLogger, state: Arc<BlaseballState>) -> Result<Arc<BlaseballState>, IngestError>;
}