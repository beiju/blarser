use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Utc};
use diesel::{self, insert_into, RunQueryDsl};
use futures::{stream, Stream, StreamExt};
use rocket::{info};

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;
use crate::state::{add_initial_versions, StateInterface};

fn initial_state(start_at_time: &'static str) -> impl Stream<Item=(&'static str, ChroniclerItem)> {
    type ChronUpdateStream = Pin<Box<dyn Stream<Item=(&'static str, ChroniclerItem)> + Send>>;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::entities(entity_type, start_at_time)
                .map(move |entity| (entity_type, entity));

            Box::pin(stream) as ChronUpdateStream
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |entity| ("game", entity))
            ) as ChronUpdateStream
        ));

    stream::select_all(streams)
}

pub async fn ingest_chron(ingest: IngestState, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    let start_time_parsed = DateTime::parse_from_rfc3339(start_at_time)
        .expect("Couldn't parse hard-coded Blarser start time")
        .with_timezone(&Utc);

    let initial_versions: Vec<_> = initial_state(start_at_time).collect().await;
    add_initial_versions(ingest.db, ingest.ingest_id, start_time_parsed, initial_versions).await;

    info!("Finished populating initial Chron values");

    // TODO: Re-add ingest
    loop {
        tokio::time::sleep(core::time::Duration::from_secs(500)).await;
        info!("Fake running ingest {}", ingest.ingest_id)
    }

}


async fn wait_for_feed_ingest(ingest: &mut IngestState, wait_until_time: DateTime<Utc>) {
    loop {
        let feed_time = *ingest.receive_progress.borrow();
        if wait_until_time < feed_time {
            break;
        }
        info!("Chronicler ingest waiting for Eventually ingest to catch up (at {} and we need {}, difference of {}s)",
            feed_time, wait_until_time, (wait_until_time - feed_time).num_seconds());
        ingest.receive_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}