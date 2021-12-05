use std::sync::Arc;
use thiserror::Error;
use crate::blaseball_state::{BlaseballState, Observation};


#[derive(Error, Debug)]
pub enum IngestError {
    #[error("Chron {observation:?} update didn't match the expected value: {diff}")]
    UpdateMismatch { observation: Observation, diff: String },

    #[error("Expected {path} to have type {expected_type}, but it had type {actual_type}")]
    UnexpectedType { path: String, expected_type: &'static str, actual_type: &'static str },

    #[error("State was missing key {0}")]
    MissingKey(String),

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