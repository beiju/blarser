use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::blaseball_state::BlaseballState;
use crate::ingest::IngestError;
use crate::ingest::log::IngestLogger;

pub trait IngestItem {
    fn date(&self) -> DateTime<Utc>;
    fn apply(&self, log: &IngestLogger, state: Arc<BlaseballState>) -> Result<Arc<BlaseballState>, IngestError>;
}

pub type BoxedIngestItem = Box<dyn IngestItem + Send + Sync>;