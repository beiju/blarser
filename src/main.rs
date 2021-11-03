use blarser::ingest;
use std::error::Error;

fn main() -> Result<(), impl Error> {
    ingest::ingest()
}
