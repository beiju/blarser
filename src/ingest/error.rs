use std::sync::Arc;
use thiserror::Error;
use crate::blaseball_state::{BlaseballState, PathError};


#[derive(Error, Debug)]
pub enum IngestError {
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
    }
}

pub type IngestResult = Result<Vec<Arc<BlaseballState>>, IngestError>;