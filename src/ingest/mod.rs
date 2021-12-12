pub mod ingest;
mod source;
mod eventually;
mod chronicler;
mod error;
mod log;
mod task;
mod text_parser;

pub use error::{IngestError, IngestResult};
pub use source::{BoxedIngestItem, IngestItem};
pub use task::IngestTask;
