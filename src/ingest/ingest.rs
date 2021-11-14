use std::sync::Arc;
use itertools::Itertools;
use log::{info};
use rocket::futures::stream::TryStreamExt;

use crate::blaseball_state::BlaseballState;
use crate::ingest::{chronicler, eventually, IngestItem};
use crate::ingest::error::IngestError;

const BLARSER_START: &str = "2021-11-01T00:00:00Z";

fn all_sources(start: &'static str) -> impl Iterator<Item=Result<Box<dyn IngestItem + Send>, IngestError>> + Send {
    [
        eventually::sources(start),
        chronicler::sources(start),
    ]
        .into_iter()
        .flatten()
        .kmerge_by(|a, b| a.date() < b.date())
        .map(|k| Ok(k))
}

pub async fn run() -> Result<Arc<BlaseballState>, IngestError> {
    info!("Starting ingest");
    let start_state = Arc::new(BlaseballState::from_chron_at_time(BLARSER_START));
    info!("Got initial state");

    rocket::futures::stream::iter(all_sources(BLARSER_START))
        .try_fold(start_state, |latest_state, ingest_item| async move {
            ingest_item.apply(latest_state).await
        })
        .await
}

