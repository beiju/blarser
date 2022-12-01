use std::cmp::Reverse;
use chrono::{DateTime, Utc};
use diesel::QueryResult;
use rocket::{info, warn};
use futures::{pin_mut, StreamExt};
use itertools::Itertools;
use uuid::Uuid;

use crate::api::{EventType, eventually, EventuallyEvent};
use crate::entity::{AnyEntity, Entity};
use crate::{entity_dispatch, with_any_event};
use crate::events::Event;
use crate::ingest::parse::parse_feed_event;

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
        let mut timed_events = state.get_events_between(start_time, end_time)?;
        // Reverse timed_events so that popping returns them in the right order
        timed_events.reverse();

        while let Some((stored_event, effects)) = timed_events.pop() {
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

            let successors = with_any_event!(stored_event.event, event => {
                for (event, effects) in event.generate_successors() {
                    let (stored_event, stored_effects) = state.save_timed_event(event, effects)?;
                    if stored_event.time > start_time && stored_event.time <= end_time {
                        // This will happen so rarely I'm not going to bother with a proper sorted
                        // insert
                        timed_events.push((stored_event, stored_effects));
                        timed_events.sort_by_key(|(e, _)| Reverse(e.time));
                        todo!("Verify the above sorting");
                    }
                }
                apply_event_effects(&state, &event, &effects)
            })?;
            state.save_successors(successors, stored_event.time, stored_event.id)?;
        }

        Ok::<_, diesel::result::Error>(())
    }).await
        .expect("Database error running time forward in feed ingest");

    ingest
}

async fn apply_feed_event(mut ingest: FeedIngest, mut feed_event: EventuallyEvent) -> FeedIngest {
    if feed_event.r#type == EventType::PlayerStatReroll {
        // I think snowfall events are the only time a PlayerStatReroll event is at the top level
        let player_id = feed_event.player_id()
            .expect("PlayerStatReroll event must have exactly one player id");
        // Unfortunately, team_id isn't set, so I need to read it from state
        let team_id = ingest.run(move |state| {
            Ok::<_, diesel::result::Error>(
                state.read_player(player_id, |player| {
                    player.league_team_id
                        .expect("Players from a PlayerStatReroll event must have a team id")
                })?
                    .into_iter()
                    .exactly_one()
                    .expect("Can't handle ambiguity in player's team")
            )
        }).await
            .expect("Error fetching player's team");
        ingest.add_pending_snowfall(team_id, feed_event);

        return ingest;
    } else if feed_event.r#type == EventType::Snowflakes {
        let mut new_events: Vec<_> = feed_event.team_tags.iter()
            .filter_map(|team_id| ingest.get_snowfalls_for_team(team_id))
            .flatten()
            .sorted_by_key(|event| event.created)
            .collect();
        // In gamma10, these were properly placed in the feed and they were before all other
        // messages, so that's what I'm doing for the stranded events too.
        new_events.append(&mut feed_event.metadata.siblings);
        feed_event.metadata.siblings = Vec::new();
        feed_event = new_events.first().unwrap().clone();
        feed_event.metadata.siblings = new_events;
    }

    ingest.run_transaction(move |state| {
        info!("Feed ingest: Applying new {:?} event at {}", feed_event.r#type, feed_event.created);
        let (event, effects) = parse_feed_event(&feed_event, &state)?;

        if effects.len() == 0 {
            warn!("{} event has no effects", event.type_name());
        }

        let successors = with_any_event!(&event, event => {
            for (event,  effects) in event.generate_successors() {
                state.save_timed_event(event, effects)?;
            }

            apply_event_effects(&state, event, &effects)
        })?;
        let (stored_event, _) = StateInterface::save_feed_event(&state, event, effects)?;
        state.save_successors(successors, stored_event.time, stored_event.id)
    }).await
        .expect("Ingest failed");

    ingest
}
