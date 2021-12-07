use std::sync::Arc;
use thiserror::Error;
use crate::blaseball_state::{ApplyChangeError, BlaseballState, PathError};


#[derive(Error, Debug)]
pub enum IngestError {
    #[error("Unexpected observation: \n{0}")]
    UnexpectedObservation(String),

    #[error("Bad event: {0}")]
    BadEvent(String),

    #[error(transparent)]
    PathError {
        #[from]
        source: PathError,
    },

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

    #[error(transparent)]
    Deserialize {
        #[from]
        source: serde_json::error::Error,
    },

    #[error(transparent)]
    ApplyChange {
        #[from]
        source: ApplyChangeError,
    }
}

pub type IngestResult<T> = Result<T, IngestError>;
pub type IngestApplyResult = IngestResult<Vec<Arc<BlaseballState>>>;