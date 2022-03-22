use chrono::{DateTime, Utc};
use rocket::info;
use futures::{pin_mut, StreamExt};

use crate::api::{eventually, EventuallyEvent};
use crate::events::{EventAux, EventTrait};
use crate::ingest::parse::parse_feed_event;
use crate::ingest::task::FeedIngest;

use crate::state::MergedSuccessors;

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

        ingest.wait_for_chron_ingest(feed_event_time).await
    }
}

async fn run_time_until(ingest: FeedIngest, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> FeedIngest {
    ingest.run_transaction(move |state| {
        // TODO: Properly handle when a timed event generates another timed event
        for (event, effects) in state.get_events_between(start_time, end_time)? {
            let mut successors = MergedSuccessors::new();

            for effect in effects {
                let aux_data: EventAux = serde_json::from_value(effect.aux_data.clone())
                    .expect("Error deserializing event aux");
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
        let (event, effects) = parse_feed_event(feed_event, &state)?;


        let mut successors = MergedSuccessors::new();
        for (entity_type, entity_id, aux_info) in &effects {
            for version in state.get_versions_at(entity_type, *entity_id, event.time())? {
                let new_entity = EventTrait::forward(&event, version.entity, &aux_info);
                successors.add_successor(version.id, (new_entity, aux_info.clone()));
            }
        }

        let stored_event = state.save_feed_event(event, effects)?;
        state.save_successors(successors.into_inner(), &stored_event)?;

        Ok::<_, diesel::result::Error>(())
    }).await
        .expect("Ingest failed");

    ingest
}
