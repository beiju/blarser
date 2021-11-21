use std::sync::Arc;
use thiserror::Error;
use crate::blaseball_state::BlaseballState;


#[derive(Error, Debug)]
pub enum IngestError {
    #[error("Chron {endpoint} update didn't match the expected value: {diff}")]
    UpdateMismatch { endpoint: &'static str, diff: String },

    #[error(transparent)]
    Io {
        #[from]
        source: std::io::Error,
    },

    #[error(transparent)]
    Database {
        #[from]
        source: diesel::result::Error,
    },
}

pub type IngestResult = Result<Arc<BlaseballState>, IngestError>;