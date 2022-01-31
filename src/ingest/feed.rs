use chrono::{DateTime, Duration, Utc};
use rocket::info;
use futures::StreamExt;

use crate::api::{eventually, EventuallyEvent};
use crate::ingest::task::IngestState;

pub async fn ingest_feed(db: IngestState, start_at_time: &'static str) {
    info!("Started Feed ingest task");

    eventually::events(start_at_time)
        .ready_chunks(50)
        .fold(db, |mut ingest, events| async move {
            let processed_up_to = save_feed_events(&ingest, events).await;

            loop {
                let stop_at = *ingest.receive_progress.borrow() + Duration::minutes(1);
                if processed_up_to < stop_at {
                    break
                }
                info!("Waiting for Chron thread to progress ({}s)", (processed_up_to - stop_at).num_seconds());
                ingest.receive_progress.changed().await
                    .expect("Communication with Chron thread failed");
            }

            ingest.notify_progress.send(processed_up_to)
                .expect("Communication with Chron thread failed");

            ingest
        }).await;
}

async fn save_feed_events(_ingest: &IngestState, events: Vec<EventuallyEvent>) -> DateTime<Utc> {
    info!("Got batch of {} events", events.len());
    events.split_last()
        .expect("save_feed_events was called with no events")
        .0.created
}