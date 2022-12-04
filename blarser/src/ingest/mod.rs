mod task;
// mod feed;
mod chron;
mod observation;
mod observation_event;
mod fed;
mod state;


use std::cmp::Reverse;

pub use task::{IngestTask, IngestTaskHolder};
pub use observation::Observation;
pub use observation_event::ChronObservationEvent;

use chrono::{DateTime, Utc};

use futures::{pin_mut, StreamExt};
use log::info;

use crate::ingest::task::Ingest;
use crate::ingest::fed::{get_fed_event_stream, get_timed_event_list, ingest_event};
use crate::ingest::chron::{chron_updates, ingest_observation, load_initial_state};
use crate::events::Event;

pub async fn run_ingest(mut ingest: Ingest, start_time: DateTime<Utc>) {
    info!("Loading initial state from {start_time}...");
    let initial_observations = load_initial_state(&ingest, start_time).await;
    {
        let mut state = ingest.state.lock().unwrap();
        state.populate(initial_observations);
    }

    let mut timed_events = get_timed_event_list(&mut ingest, start_time).await;
    info!("Initial state has {} timed events:", timed_events.len());
    for evt in &timed_events {
        info!(" - {}", evt.0);
    }

    let fed_events = get_fed_event_stream().peekable();
    pin_mut!(fed_events);
    let observations = chron_updates(start_time).peekable();
    pin_mut!(observations);

    info!("Starting ingest");
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
