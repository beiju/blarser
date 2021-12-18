use std::cmp::Ordering;
use std::sync::Arc;
use chrono::{DateTime, Utc};

use crate::blaseball_state::BlaseballState;
use crate::ingest::IngestResult;
use crate::ingest::log::IngestLogger;

pub type IngestApplyResult = IngestResult<(Arc<BlaseballState>, Vec<Box<dyn IngestItem>>)>;

pub trait IngestItem {
    fn date(&self) -> DateTime<Utc>;
    fn apply(&self, log: &IngestLogger, state: Arc<BlaseballState>) -> IngestApplyResult;
}

impl Ord for dyn IngestItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.date().cmp(&other.date()).reverse()
    }
}

impl Eq for dyn IngestItem {}

impl PartialEq<Self> for dyn IngestItem {
    fn eq(&self, other: &Self) -> bool {
        self.date().eq(&other.date())
    }
}

impl PartialOrd<Self> for dyn IngestItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.date().cmp(&other.date()).reverse())
    }
}

pub type BoxedIngestItem = Box<dyn IngestItem + Send + Sync>;