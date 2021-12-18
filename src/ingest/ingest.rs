use std::collections::BinaryHeap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use crate::blaseball_state::{BlaseballState, json_path};
use crate::ingest::{chronicler, eventually, BoxedIngestItem, IngestItem, IngestResult, IngestError};
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

pub fn run(log: IngestLogger<'_>) -> Result<(), IngestError> {
    let start_state = Arc::new(BlaseballState::from_chron_at_time(BLARSER_START));
    log.info("Got initial state".to_string())?;

    // make the move block move a reference to log instead of the actual object
    let log = &log;
    let mut internal_events = BinaryHeap::new();
    for event in get_initial_internal_events(&start_state, BLARSER_START)? {
        internal_events.push(event);
    }

    all_sources(BLARSER_START)
        .try_fold(start_state, |mut state, ingest_result| {
            // TODO Why is ingest_item not unwrapped by virtue of being inside a `try_fold`?
            let ingest_item = ingest_result?;

            while let Some(internal_item) = internal_events.peek() {
                if internal_item.date() < ingest_item.date() {
                    let (new_state, new_internal_events) = internal_events.pop().unwrap().apply(log, state)?;
                    state = new_state;
                    for event in new_internal_events {
                        internal_events.push(event);
                    }
                } else {
                    break
                }
            }

            let (new_state, new_internal_events) = ingest_item.apply(log, state)?;
            state = new_state;
            for event in new_internal_events {
                internal_events.push(event);
            }

            Ok::<_, IngestError>(state)
        })?;

    Ok(())
}

fn get_initial_internal_events(state: &Arc<BlaseballState>, date_started: &'static str) -> IngestResult<Vec<Box<dyn IngestItem>>> {
    let date_started = parse_date(date_started)?;

    let earlseason_date = parse_date(&state.string_at(&json_path!("sim", uuid::Uuid::nil(), "earlseasonDate"))?)?;

    let mut events: Vec<Box<dyn IngestItem>> = Vec::new();
    if earlseason_date > date_started {
        events.push(Box::new(StartSeasonItem::new(earlseason_date)));
    }

    Ok(events)
}

fn parse_date(date_started: &str) -> IngestResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(date_started)?.with_timezone(&Utc))
}

