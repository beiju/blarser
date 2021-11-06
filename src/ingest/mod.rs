pub mod ingest;
// pub mod parse;
mod source;
mod eventually;
mod chronicler;
mod error;

pub use error::IngestError;
pub use source::IngestItem;
pub use ingest::run;