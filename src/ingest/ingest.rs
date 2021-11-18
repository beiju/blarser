use std::sync::Arc;
use itertools::Itertools;
use rocket::futures::stream::TryStreamExt;

use crate::db::BlarserDbConn;
use crate::blaseball_state::BlaseballState;
use crate::ingest::{chronicler, eventually, IngestItem};
use crate::ingest::error::IngestError;
use crate::ingest::log::IngestLog;

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

pub async fn run(client: BlarserDbConn) -> Result<Arc<BlaseballState>, IngestError> {
    let log = IngestLog::new(client).await?;

    let start_state = Arc::new(BlaseballState::from_chron_at_time(BLARSER_START));
    log.info("Got initial state".to_string()).await?;

    rocket::futures::stream::iter(all_sources(BLARSER_START))
        .try_fold(start_state, |latest_state, ingest_item| async move {
            ingest_item.apply(latest_state).await
        })
        .await
}

