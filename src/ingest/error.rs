use thiserror::Error;


#[derive(Error, Debug)]
pub enum IngestError {
    #[error("Chron {endpoint} update didn't match the expected value: {diff}")]
    UpdateMismatch { endpoint: &'static str, diff: String },

    #[error(transparent)]
    Io {
        #[from]
        source: std::io::Error,
    },
}
