mod task;
// mod feed;
mod chron;
mod observation;
mod observation_event;
mod fed;
mod state;

use std::borrow::{Borrow, BorrowMut};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
pub use task::{IngestTask, IngestTaskHolder};
pub use observation::Observation;
pub use observation_event::ChronObservationEvent;

use chrono::{DateTime, Utc};
use env_logger::init;
use futures::{pin_mut, StreamExt};
use log::info;
use rocket::http::ext::IntoCollection;
use serde_json::de::Read;
use tokio::sync::Mutex;

use crate::ingest::task::Ingest;
use crate::ingest::fed::{get_fed_event_stream, get_timed_event_list, ingest_event};
use crate::ingest::chron::{chron_updates, ingest_observation, load_initial_state, ObservationStreamWithCursor};
use crate::events::{AnyEvent, Event};
use crate::ingest::state::StateGraph;

pub async fn run_ingest(mut ingest: Ingest, start_time: DateTime<Utc>) {
    info!("Loading initial state...");
    let initial_observations = load_initial_state(&ingest, start_time).await;
    {
        let mut state = ingest.state.lock().unwrap();
        state.populate(initial_observations);
    }
    info!("Started ingest task");

    let fed_events = get_fed_event_stream().peekable();
    pin_mut!(fed_events);
    let observations = chron_updates(start_time).peekable();
    pin_mut!(observations);

    let mut timed_events = get_timed_event_list(&mut ingest, start_time).await;

    loop {
        let next_fed_event_time = {
            let mut time = fed_events.as_mut().peek().await
                .expect("This stream should never terminate")
                .last_update_time();

            // Consume all the empty ingests from fed_events
            while fed_events.as_mut().peek().await.expect("This stream should never terminate").event().is_none() {
                let item = fed_events.next().await.unwrap();
                assert!(item.event().is_none());
                time = item.last_update_time();
            }

            time
        };

        let next_timed_event_time = timed_events.peek()
            .map(|event| event.0.time());

        let next_observation_time = observations.as_mut().peek().await
            .expect("This stream should never terminate")
            .latest_time();

        if next_observation_time < next_fed_event_time {
            let observation = observations.next().await
                .expect("This stream should never terminate");
            let new_timed_events = ingest_observation(&mut ingest, observation);
            timed_events.extend(new_timed_events.into_iter().map(Reverse));
        } else {
            let event = fed_events.next().await
                .expect("This stream should never terminate")
                .into_event()
                .expect("Should always have an event at this stage");
            let new_timed_events = ingest_event(&mut ingest, event);
            timed_events.extend(new_timed_events.into_iter().map(Reverse));
        }
    }
}
