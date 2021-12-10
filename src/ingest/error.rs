use std::sync::Arc;
use crate::blaseball_state::{BlaseballState};

pub type IngestError = anyhow::Error;
pub type IngestResult<T> = Result<T, IngestError>;
pub type IngestApplyResult = IngestResult<Vec<Arc<BlaseballState>>>;