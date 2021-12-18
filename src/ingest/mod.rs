pub mod ingest;
mod source;
mod eventually;
mod chronicler;
mod error;
mod log;
mod task;
mod text_parser;
mod internal_events;
mod data_views;

pub use error::{IngestError, IngestResult};
pub use source::{BoxedIngestItem, IngestItem, IngestApplyResult};
pub use task::IngestTask;
