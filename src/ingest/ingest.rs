use std::sync::Arc;
use chrono::Utc;
use itertools::Itertools;
use log::{info};
use rocket::futures::stream::TryStreamExt;
use diesel::prelude::*;

use crate::blaseball_state::BlaseballState;
use crate::db::{BlarserDbConn, NewIngest, Ingest};
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

pub async fn run(client: BlarserDbConn) -> Result<Arc<BlaseballState>, IngestError> {
    use crate::schema::ingests::dsl::*;
    let this_ingest: Ingest = client.run(move |c|
        diesel::insert_into(ingests).values(NewIngest {
            started_at: Utc::now().naive_utc()
        }).get_result(c)
    ).await?;

    info!("Starting ingest {} at {}", this_ingest.id, this_ingest.started_at);

    let start_state = Arc::new(BlaseballState::from_chron_at_time(BLARSER_START));
    info!("Got initial state");

    rocket::futures::stream::iter(all_sources(BLARSER_START))
        .try_fold(start_state, |latest_state, ingest_item| async move {
            ingest_item.apply(latest_state).await
        })
        .await
}

