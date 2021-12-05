use std::sync::Arc;
use itertools::Itertools;
use rocket::futures::stream::{StreamExt, TryStreamExt};

use crate::blaseball_state::BlaseballState;
use crate::ingest::{chronicler, eventually, BoxedIngestItem};
use crate::ingest::error::IngestError;
use crate::ingest::log::IngestLogger;

const BLARSER_START: &str = "2021-11-01T00:00:00Z";

fn all_sources(start: &'static str) -> impl Iterator<Item=Result<BoxedIngestItem, IngestError>> + Send {
    [
        eventually::sources(start),
        chronicler::sources(start),
    ]
        .into_iter()
        .flatten()
        .kmerge_by(|a, b| a.date() < b.date())
        .map(|k| Ok(k))
}

pub async fn run(log: IngestLogger) -> Result<(), IngestError> {
    let start_states = vec![
        Arc::new(BlaseballState::from_chron_at_time(BLARSER_START))
    ];
    log.info("Got initial state".to_string()).await?;

    // make the move block move a reference to log instead of the actual object
    let log = &log;
    rocket::futures::stream::iter(all_sources(BLARSER_START))
        .try_fold(start_states, |states, ingest_item| async move {
            rocket::futures::stream::iter(states)
                .then(|state| ingest_item.apply(log, state))
                .collect::<Vec<Result<_, IngestError>>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, IngestError>>()
                .map(|v| v.into_iter().flatten().collect())
        })
        .await?;

    Ok(())
}

