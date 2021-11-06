use std::rc::Rc;
use itertools::Itertools;
use chrono::SecondsFormat;
use log::{error, info};

use crate::blaseball_state::BlaseballState;
use crate::ingest::{chronicler, eventually, IngestItem};
use crate::ingest::error::IngestError;

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

fn all_sources(start: &'static str) -> impl Iterator<Item=Box<dyn IngestItem>> {
    [
        eventually::sources(start),
        chronicler::sources(start),
    ]
        .into_iter()
        .flatten()
        .kmerge_by(|a, b| a.date() < b.date())
}

pub fn run() -> Result<Rc<BlaseballState>, IngestError> {
    info!("Starting ingest");
    let start_state = Rc::new(BlaseballState::from_chron_at_time(EXPANSION_ERA_START));
    info!("Got initial state");

    all_sources(EXPANSION_ERA_START).into_iter()
        .try_fold(start_state, |latest_state, ingest_item| {
            ingest_item.apply(latest_state)
        })
}

