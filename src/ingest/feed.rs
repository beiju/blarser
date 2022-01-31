use chrono::{DateTime, Duration, Utc};
use rocket::info;
use futures::StreamExt;

use crate::api::{eventually, EventuallyEvent};
use crate::ingest::task::IngestState;

pub async fn ingest_feed(db: IngestState, start_at_time: &'static str) {
    info!("Started Feed ingest task");

    let start_time_parsed = DateTime::parse_from_rfc3339(start_at_time)
        .expect("Couldn't parse Blarser start time")
        .with_timezone(&Utc) + Duration::hours(1);

    eventually::events(start_at_time)
        .ready_chunks(50)
        .fold((start_time_parsed, db), |(mut pause_at, mut ingest), events| async move {
            let processed_up_to = save_feed_events(&ingest, events).await;

            // while processed_up_to > pause_at {
            //     pause_at = ingest.receive_progress.recv().await
            //         .expect("Communication with Chron thread failed") + Duration::hours(1);
            // }

            (processed_up_to, ingest)
        }).await;
}

async fn save_feed_events(_ingest: &IngestState, events: Vec<EventuallyEvent>) -> DateTime<Utc> {
    info!("Got batch of {} events", events.len());
    events.split_last()
        .expect("save_feed_events was called with no events")
        .0.created
}