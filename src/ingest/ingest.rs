use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use rocket::futures::stream::TryStreamExt;

use crate::blaseball_state::{BlaseballState, json_path};
use crate::ingest::{chronicler, eventually, BoxedIngestItem, IngestItem, IngestResult};
use crate::ingest::error::IngestError;
use crate::ingest::internal_events::StartSeasonItem;
use crate::ingest::log::IngestLogger;

// Current start: beginning of gamma 2. After adding game update support, it's no longer valid to
// start parsing blaseball in the middle of a season. (And for now, it has to be after the season
// schedule is generated.)
const BLARSER_START: &str = "2021-12-06T15:00:00Z";

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
    let start_state = Arc::new(BlaseballState::from_chron_at_time(BLARSER_START));
    log.info("Got initial state".to_string()).await?;

    // make the move block move a reference to log instead of the actual object
    let log = &log;
    rocket::futures::stream::iter(all_sources(BLARSER_START))
        .try_fold((start_state, None), |(state, last_update), ingest_item| async move {
            let state = if let Some(internal_event) = get_internal_event(&state, last_update,ingest_item.date()).await? {
                internal_event.apply(log, state).await?
            } else {
                state
            };

            Ok((ingest_item.apply(log, state).await?, Some(ingest_item.date())))
        })
        .await?;

    Ok(())
}

async fn get_internal_event(state: &Arc<BlaseballState>, last_update: Option<DateTime<Utc>>, next_update: DateTime<Utc>) -> IngestResult<Option<impl IngestItem>> {
    let sim_start_date = state.string_at(&json_path!("sim", uuid::Uuid::nil(), "earlseasonDate")).await?;
    let sim_start_date = DateTime::parse_from_rfc3339(&sim_start_date)?;
    let sim_start_date = sim_start_date.with_timezone(&Utc);

    let is_after_last_update = if let Some(last_update) = last_update {
        sim_start_date > last_update
    } else {
        true
    };

    if is_after_last_update && sim_start_date < next_update {
        Ok(Some(StartSeasonItem::new(sim_start_date)))
    } else {
        Ok(None)
    }
}

