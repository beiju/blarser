use anyhow::Result;

fn main() -> Result<()> {
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .init();

    blarser::process::run()?;
    Ok(())
}
