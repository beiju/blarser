use blarser::ingest;
use anyhow::Result;

fn main() -> Result<()> {
    ingest::ingest()?;
    Ok(())
}
