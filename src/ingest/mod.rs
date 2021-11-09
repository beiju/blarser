pub mod ingest;
mod source;
mod eventually;
mod chronicler;
mod error;

pub use error::IngestError;
pub use source::IngestItem;
pub use ingest::run;