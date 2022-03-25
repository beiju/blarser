use chrono::{DateTime, Utc};
use diesel::QueryResult;
use rocket::{info, warn};
use futures::{pin_mut, StreamExt};
use uuid::Uuid;

use crate::api::{EventType, eventually, EventuallyEvent};
use crate::entity::{AnyEntity, Entity};
use crate::{entity_dispatch, with_any_event};
use crate::events::Event;
use crate::ingest::parse::parse_feed_event;
use crate::ingest::task::FeedIngest;

use crate::state::{MergedSuccessors, StateInterface};

pub async fn ingest_feed(mut ingest: FeedIngest, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    info!("Started Feed ingest task");

    let feed_events = eventually::events(start_at_time);

    pin_mut!(feed_events);

    let mut current_time = start_time_parsed;

    while let Some(feed_event) = feed_events.next().await {
        info!("Feed ingest: Received new {:?} event from {}", feed_event.r#type, feed_event.created);
        let feed_event_time = feed_event.created;
        // Doing a "manual borrow" of ingest because I can't figure out how to please the borrow
        // checker with a proper borrow
        ingest = run_time_until(ingest, current_time, feed_event_time).await;
        ingest = apply_feed_event(ingest, feed_event).await;
        current_time = feed_event_time;

        ingest.wait_for_chron_ingest(feed_event_time).await
    }
}

fn apply_event_effect<EntityT: Entity, EventT: Event>(
    state: &StateInterface,
    successors: &mut MergedSuccessors<(AnyEntity, serde_json::Value, Vec<DateTime<Utc>>)>,
    entity_id: Option<Uuid>,
    event: &EventT,
    aux_info: &serde_json::Value,
) -> QueryResult<()> {
    for version in state.get_versions_at::<EntityT>(entity_id, event.time())? {
        let new_entity = event.forward(version.entity.into(), aux_info.clone());
        successors.add_successor(version.id, (new_entity, aux_info.clone(), vec![]));
    }

    Ok(())
}

fn apply_event_effects<'a, EventT: Event>(
    state: &StateInterface,
    event: &EventT,
    effects: impl IntoIterator<Item=&'a (String, Option<Uuid>, serde_json::Value)>,
) -> QueryResult<Vec<((AnyEntity, serde_json::Value, Vec<DateTime<Utc>>), Vec<i32>)>> {
    let mut successors = MergedSuccessors::new();

    for (entity_type, entity_id, aux_info) in effects {
        if let Some(entity_id) = entity_id {
            info!("Feed ingest: Applying event to {} {} with aux_info {:?}",
                  entity_type, entity_id, aux_info);
        } else {
            info!("Feed ingest: Applying event to all {} entities with aux_info {:?}",
                  entity_type, aux_info);
        }
        entity_dispatch!(entity_type.as_str() => apply_event_effect::<EventT>(state, &mut successors, *entity_id, event, aux_info);
                         other => panic!("Tried to apply event to unknown entity type {}", other))?;
    }

    Ok(successors.into_inner())
}

async fn run_time_until(ingest: FeedIngest, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> FeedIngest {
    ingest.run_transaction(move |state| {
        // TODO: Properly handle when a timed event generates another timed event
        for (stored_event, effects) in state.get_events_between(start_time, end_time)? {
            info!("Feed ingest: Applying stored {} event at {}",
                stored_event.event.type_name(), stored_event.event.time());
            let effects: Vec<_> = effects.into_iter()
                .map(|effect| {
                    let aux_data = serde_json::from_value(effect.aux_data)
                        .expect("Failed to parse aux_data from database");
                    (effect.entity_type, effect.entity_id, aux_data)
                })
                .collect();

            if effects.len() == 0 {
                warn!("{} event has no effects", stored_event.event.type_name());
            }

            let successors = with_any_event!(stored_event.event, event => apply_event_effects(&state, &event, &effects))?;
            state.save_successors(successors, stored_event.time, stored_event.id)?;
        }

        Ok::<_, diesel::result::Error>(())
    }).await
        .expect("Database error running time forward in feed ingest");

    ingest
}

async fn apply_feed_event(ingest: FeedIngest, feed_event: EventuallyEvent) -> FeedIngest {
    // FOR DEBUGGING: Pause as soon as we've ingested enough for the first Chron ingest to kick off
    // if feed_event.created.to_string().as_str() > "2021-12-06 16:00:05.056 UTC" {
    //     info!("Feed ingest: pausing forever for debug");
    //     loop {
    //         tokio::time::sleep(tokio::time::Duration::from_secs(100000)).await;
    //     }
    // }
    ingest.run_transaction(move |state| {
        info!("Feed ingest: Applying new {:?} event at {}", feed_event.r#type, feed_event.created);
        let (event, effects) = parse_feed_event(&feed_event, &state)?;

        if effects.len() == 0 {
            warn!("{} event has no effects", event.type_name());
        }

        let successors = with_any_event!(&event, event => apply_event_effects(&state, event, &effects))?;
        let stored_event = StateInterface::save_feed_event(&state, event, effects)?;
        state.save_successors(successors, stored_event.time, stored_event.id)
    }).await
        .expect("Ingest failed");

    ingest
}
