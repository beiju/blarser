pub type IngestError = anyhow::Error;
pub type IngestResult<T> = Result<T, IngestError>;
