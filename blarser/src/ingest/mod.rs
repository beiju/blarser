mod task;
// mod feed;
mod chron;
mod observation;
mod observation_event;
mod fed;
mod state;
mod error;

use std::cmp::Reverse;
use std::ops::Deref;

pub use task::{IngestTask, IngestTaskHolder};
pub use observation::Observation;
pub use observation_event::ChronObservationEvent;
pub use state::StateGraph;

use chrono::{DateTime, Utc};
use diesel::internal::operators_macro::FieldAliasMapper;

use futures::{pin_mut, StreamExt};
use log::info;

pub use crate::ingest::task::{Ingest, GraphDebugHistorySync, GraphDebugHistory};
use crate::ingest::fed::{EventStreamItem, get_fed_event_stream, get_timed_event_list, ingest_event};
use crate::ingest::chron::{chron_updates, ingest_observation, load_initial_state};
use crate::events::Event;

#[derive(Debug)]
enum Source {
    Feed,
    Timed,
    Observation,
}

pub async fn run_ingest(mut ingest: Ingest, start_time: DateTime<Utc>) {
    info!("Loading initial state from {start_time}...");
    let initial_observations = load_initial_state(&ingest, start_time).await;
    {
        let mut history = ingest.debug_history.lock().await;
        let mut state = ingest.state.lock().unwrap();

        state.populate(initial_observations, start_time, &mut *history);
    }

    let mut timed_events = get_timed_event_list(&mut ingest, start_time).await;
    info!("Initial state has {} timed events:", timed_events.len());
    for evt in &timed_events {
        info!(" - {}", evt.0);
    }

    info!("Getting fed events stream");
    let fed_events = get_fed_event_stream().peekable();
    pin_mut!(fed_events);
    info!("Getting updates stream");
    let observations = chron_updates(start_time).peekable();
    info!("Got updates stream");
    pin_mut!(observations);

    let mut latest_feed_update = start_time;

    loop {
        // TODO this always blocks until the next event comes in, defeating the purpose of having
        //   event-less "latest ingest time" updates
        info!("Finding next feed event time");
        let next_fed_event_time = loop {
            // Consume all the empty ingests from fed_events
            let next_item: &EventStreamItem = fed_events.as_mut().peek().await
                .expect("This stream should never terminate");
            latest_feed_update = next_item.last_update_time();
            if let Some(event) = next_item.event() {
                break event.time()
            } else {
                info!("Skipping empty event");
                let n: EventStreamItem = fed_events.as_mut().next().await
                    .expect("This stream should never terminate");
                assert!(n.event().is_none(),
                        "This part of the loop should only ever drain items with no event");
            }
        };
        info!("Next feed event is at {next_fed_event_time}");

        info!("Getting next timed event time");
        let next_timed_event_time = timed_events.peek()
            .map(|event| event.0.time());
        if let Some(t) = next_timed_event_time {
            info!("Next timed event is at {t}");
        } else {
            info!("No next time events");
        }

        // TODO Allow this to be None if there are currently no observations
        info!("Getting next observation time");
        let next_observation_time = observations.as_mut().peek().await
            .expect("This stream should never terminate")
            .latest_time();
        info!("Next observation is at {next_observation_time}");

        info!("Selecting source");
        let Some((source, time)) = [
            Some((Source::Feed, next_fed_event_time)),
            next_timed_event_time.map(|t| (Source::Timed, t)),
            Some((Source::Observation, next_observation_time))
        ].into_iter()
            .flatten() // Get rid of None options
            .min_by_key(|(_, time)| *time) else {
            todo!(); // should this ever happen?
        };
        info!("Selected {source:?}");

        if time > latest_feed_update {
            info!("Caught up with the Feed");
            continue;
        }

        let new_timed_events = match source {
            Source::Feed => {
                let event = fed_events.next().await
                    .expect("This stream should never terminate")
                    .into_event()
                    .expect("If we got here, the source should not be empty");
                ingest_event(&mut ingest, event).await.unwrap()
            }
            Source::Timed => {
                let event = timed_events.pop()
                    .expect("If we got here, the source should not be empty").0;
                ingest_event(&mut ingest, event).await.unwrap()
            }
            Source::Observation => {
                let observation = observations.next().await
                    .expect("This stream should never terminate");
                ingest_observation(&mut ingest, observation)
            }
        };

        timed_events.extend(new_timed_events.into_iter().map(Reverse));

    }
    info!("Starting ingest");
}
