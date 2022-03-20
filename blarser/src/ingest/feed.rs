use chrono::{DateTime, Duration, Utc};
use rocket::info;
use futures::{pin_mut, StreamExt};

use crate::api::{eventually, EventuallyEvent};
use crate::events::{Event, EventAux, EventTrait};
use crate::ingest::parse::parse_feed_event;
use crate::ingest::task::FeedIngest;

use crate::state::{add_feed_event, MergedSuccessors, NewVersion};

pub async fn ingest_feed(mut ingest: FeedIngest, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    info!("Started Feed ingest task");

    let feed_events = eventually::events(start_at_time);

    pin_mut!(feed_events);

    let mut current_time = start_time_parsed;

    while let Some(feed_event) = feed_events.next().await {
        let feed_event_time = feed_event.created;
        // Doing a "manual borrow" of ingest because I can't figure out how to please the borrow
        // checker with a proper borrow
        ingest = run_time_until(ingest, current_time, feed_event_time).await;
        ingest = apply_feed_event(ingest, feed_event).await;
        current_time = feed_event_time;

        wait_for_chron_ingest(&mut ingest, feed_event_time).await
    }
}

async fn run_time_until(ingest: FeedIngest, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> FeedIngest {
    ingest.run_transaction(move |state| {
        // TODO: Properly handle when a timed event generates another timed event
        for (event, effects) in state.get_events_between(start_time, end_time)? {
            let mut successors = MergedSuccessors::new();

            for effect in effects {
                let aux_data = event.event.deserialize_aux(effect.aux_data);
                for version in state.get_latest_versions(&effect.entity_type, effect.entity_id)? {
                    let new_entity = event.event.forward(version.entity, &aux_data);
                    successors.add_successor(version.id, (new_entity, aux_data.clone()));
                }
            }

            state.save_successors(successors.into_inner(), &event)?;
        }

        Ok::<_, diesel::result::Error>(())
    }).await
        .expect("Database error running time forward in feed ingest");

    ingest
}

async fn apply_feed_event(ingest: FeedIngest, feed_event: EventuallyEvent) -> FeedIngest {
    ingest.run_transaction(move |state| {
        let (event, effects) = parse_feed_event(feed_event, state)?;

        state.save_feed_event(event, effects)?;

        let mut successors = MergedSuccessors::new();
        for (entity_type, entity_id, aux_info) in effects {
            for version in state.get_versions(entity_type, entity_id, event.time())? {
                let new_entity = event.forward(version.entity, aux_info);
                successors.add_successors(version.id, (new_entity, aux_info));
            }
        }

        state.save_versions(successors.into_inner())?;

        Ok(())
    }).await
        .expect("Ingest failed");

    ingest
}

async fn wait_for_chron_ingest(ingest: &mut FeedIngest, feed_event_time: DateTime<Utc>) {
    ingest.send_feed_progress.send(feed_event_time)
        .expect("Error communicating with Chronicler ingest");
    info!("Feed ingest sent progress {}", feed_event_time);

    loop {
        let chron_requests_time = *ingest.receive_chron_progress.borrow();
        let stop_at = chron_requests_time + Duration::seconds(1);
        if feed_event_time < stop_at {
            break;
        }
        info!("Eventually ingest waiting for Chronicler ingest to catch up (at {} and we are at {}, {}s ahead)",
                    chron_requests_time, feed_event_time, (feed_event_time - chron_requests_time).num_seconds());
        ingest.receive_chron_progress.changed().await
            .expect("Error communicating with Chronicler ingest");
    }
}