use blarser::ingest;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    ingest::ingest()
}
