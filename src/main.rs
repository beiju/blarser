use blarser::ingest;
use anyhow::Result;

fn main() -> Result<()> {
    pretty_env_logger::formatted_builder()
        .target(pretty_env_logger::env_logger::Target::Stdout)
        .init();

    ingest::ingest()?;
    Ok(())
}
