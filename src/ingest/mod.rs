pub mod ingest;
mod source;
mod eventually;
mod chronicler;
mod error;
mod log;
mod task;

pub use error::{IngestError, IngestResult};
pub use source::{IngestItem, BoxedIngestItem};
pub use task::IngestTask;