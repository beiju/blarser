use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::{Display, Formatter};
use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Utc};
use diesel::QueryResult;
use futures::{pin_mut, stream, Stream, StreamExt};
use itertools::{EitherOrBoth, Itertools};
use rocket::{info, warn};
use serde_json::Value;
use thiserror::Error;
use partial_information::Conflict;

use crate::api::chronicler;
use crate::ingest::task::{ChronIngest, IsSerializationFailure};
use crate::entity::{AnyEntity, Entity, EntityParseError, EntityRaw};
use crate::ingest::observation::Observation;
use crate::state::{EventEffect, MergedSuccessors, NewVersion, StateInterface, Version, VersionLink};
use crate::{with_any_entity_raw, with_any_event};
use crate::events::Event;

fn initial_state(start_at_time: &'static str) -> impl Stream<Item=Observation> {
    type ObservationStream = Pin<Box<dyn Stream<Item=Observation> + Send>>;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::entities(entity_type, start_at_time)
                // The whole purpose of the filter_map is to silently ignore UnknownEntity errors,
                // because it's a pain to write the data structure to properly deserialize a whole
                // entity type and I want to defer it until I actually implement the entity.
                // It's async because the signature of filter_map requires it
                .filter_map(move |item| async {
                    match Observation::from_chron(entity_type, item) {
                        Err(EntityParseError::UnknownEntity(_)) => None,
                        other => Some(other.unwrap()),
                    }
                });

            Box::pin(stream) as ObservationStream
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |item| Observation::from_chron("game", item).unwrap())
            ) as ObservationStream
        ));

    stream::select_all(streams)
}

type PinnedObservationStream = Pin<Box<dyn Stream<Item=Observation> + Send>>;

fn chron_updates(start_at_time: &'static str) -> impl Stream<Item=Observation> {
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                // See note on equivalent function in initial_state
                .filter_map(move |item| async {
                    match Observation::from_chron(entity_type, item) {
                        Err(EntityParseError::UnknownEntity(_)) => None,
                        other => Some(other.unwrap()),
                    }
                });

            Box::pin(stream) as PinnedObservationStream
        })
        .chain(iter::once({
            let stream = chronicler::game_updates(start_at_time)
                .map(|item| Observation::from_chron("game", item).unwrap());

            Box::pin(stream) as PinnedObservationStream
        }));

    kmerge_stream(streams)
}

fn kmerge_stream(streams: impl Iterator<Item=PinnedObservationStream>) -> impl Stream<Item=Observation> {
    let peekable_streams: Vec<_> = streams
        .map(|s| (
            s.fuse(),
            BinaryHeap::with_capacity(100)
        ))
        .collect();

    stream::unfold(peekable_streams, |mut streams| async {
        // Refill caches
        for (stream, cache) in &mut streams {
            while cache.len() < 100 {
                if let Some(next) = stream.next().await {
                    cache.push(Reverse(next));
                } else {
                    break; // Avoids infinite loop
                }
            }
        }

        let selected_stream = *streams.iter()
            .enumerate()
            .filter_map(|(i, (_, cache))| {
                cache.peek().map(|Reverse(v)| (i, v))
            })
            .collect::<Vec<_>>()
            .iter()
            .min_by_key(|(_, date)| date)
            .map(|(i, _)| i)
            .expect("TODO: Handle end of all streams");

        let (_, cache) = &mut streams[selected_stream];
        let Reverse(next) = cache.pop()
            .expect("selected_stream should never refer to a stream that doesn't have a next element");

        Some((next, streams))
    })
}

pub async fn init_chron(ingest: &ChronIngest, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    let initial_versions: Vec<_> = initial_state(start_at_time).collect().await;

    ingest.run(move |state| {
        state.add_initial_versions(start_time_parsed, initial_versions.into_iter())
    }).await
        .expect("Failed to save initial versions");

    info!("Finished populating initial Chron values");
}

pub async fn ingest_chron(mut ingest: ChronIngest, start_at_time: &'static str, start_time: DateTime<Utc>) {
    info!("Started Chron ingest task");

    let updates = chron_updates(start_at_time);

    pin_mut!(updates);

    let mut prev_observation_time = start_time;

    while let Some(observation) = updates.next().await {
        // Just to ensure observations are processed in latest_time order
        assert!(observation.latest_time() >= prev_observation_time,
                "Observations were not processed in chronological order");
        prev_observation_time = observation.latest_time();

        ingest.wait_for_feed_ingest(observation.latest_time()).await;

        let normal_ingest_result = {
            let observation = observation.clone();
            ingest.run_transaction(move |state| {
                with_any_entity_raw!(&observation.entity_raw, raw => {
                    forward_ingest(&state, raw, observation.perceived_at)?;

                    reverse_ingest(&state, raw, observation.perceived_at)?;
                });

                Ok::<_, ChronIngestError>(())
            }).await
        };

        match normal_ingest_result {
            Err(ChronIngestError::Conflicts(conflicts)) => {
                let conflicts_str = conflicts.to_string();
                warn!("Getting approval for conflicts: {}", conflicts_str);
                let approved = ingest.get_approval(observation.entity_raw.name(), observation.entity_raw.id(),
                                                   observation.perceived_at, conflicts_str).await
                    .expect("Error in get_approval");
                if !approved {
                    panic!("User rejected conflicts. Nothing to do.");
                } else {
                    ingest.run_transaction(move |state| {
                        with_any_entity_raw!(&observation.entity_raw, raw => {
                            add_manual_event(&state, raw, observation.perceived_at)
                        })
                    }).await
                        .expect("Error adding approved manual event");
                }
            }
            other => other.expect("Error in Chron ingest")
        }
    }

    todo!()
}

#[derive(Debug)]
pub struct GenerationConflict {
    start_time: DateTime<Utc>,
    event_name: &'static str,
    version_conflicts: Vec<Vec<Conflict>>,
}

#[derive(Debug)]
pub struct GenerationConflicts(Vec<GenerationConflict>);

impl Display for GenerationConflicts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Couldn't apply observation: ")?;
        for generation in &self.0 {
            write!(f, "\n- Couldn't apply to generation at {}, created by {}:",
                   generation.start_time, generation.event_name)?;

            for version in &generation.version_conflicts {
                write!(f, "\n  - Couldn't apply to version:")?;

                for conflict in version {
                    write!(f, "\n    - {}", conflict)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ChronIngestError {
    #[error("Observation could not be applied without conflicts")]
    Conflicts(GenerationConflicts),

    #[error(transparent)]
    DbError(#[from] diesel::result::Error),
}

impl IsSerializationFailure for ChronIngestError {
    fn is_serialization_failure(&self) -> bool {
        match self {
            ChronIngestError::DbError(db_err) => db_err.is_serialization_failure(),
            _ => false
        }
    }
}

pub type ChronIngestResult<T> = Result<T, ChronIngestError>;

fn forward_ingest<EntityRawT: EntityRaw>(state: &StateInterface, entity_raw: &EntityRawT, perceived_at: DateTime<Utc>) -> ChronIngestResult<()> {
    let earliest_time = entity_raw.earliest_time(perceived_at);
    let latest_time = entity_raw.latest_time(perceived_at);
    let events = state.get_events_for_versions_after(entity_raw, earliest_time)?;
    let generations = state.get_versions_for_entity_raw_between(entity_raw, earliest_time, latest_time)?;

    info!("Chron ingest: Applying observation to {} {} between {} and {}. {} generations, {} events",
        EntityRawT::name(), entity_raw.id(), earliest_time, latest_time, generations.len(), events.len());

    // The generation at the end of the window should be terminated after the whole process
    let ids_to_terminate: Vec<_> = generations.last()
        .expect("Chron ingest found zero generations in the observation window")
        .1.iter()
        .map(|(version, _)| version.id)
        .collect();

    let mut prev_generation = Vec::new();
    let mut all_conflicts = Vec::new();
    for either_or_both in events.into_iter().zip_longest(generations) {
        let ((event, effects), existing_versions) = match either_or_both {
            EitherOrBoth::Both((event, effects), (event_id, versions)) => {
                assert_eq!(event_id, event.id, "Generation's event_id did not match expected event");
                ((event, effects), Some(versions))
            }
            EitherOrBoth::Left(event_effects) => {
                (event_effects, None)
            }
            EitherOrBoth::Right(_) => {
                panic!("Got a generation without the corresponding event");
            }
        };

        let mut new_generation = MergedSuccessors::new();

        if let Some(versions) = existing_versions {
            let version_time = versions.first().expect("Empty generation").0.start_time;
            let num_versions = versions.len();
            let version_conflicts = observe_generation(&mut new_generation, versions, entity_raw, perceived_at);
            info!("Chron ingest: Generation at {} with {} versions observed, resulting in {} successors and {} conflicts",
            version_time, num_versions, new_generation.inner().len(), version_conflicts.len());

            all_conflicts.push(GenerationConflict {
                start_time: version_time,
                event_name: event.event.type_name(),
                version_conflicts,
            });
        }

        let num_prev_versions = prev_generation.len();
        let num_successors_before = new_generation.inner().len();
        with_any_event!(event.event, event => advance_generation(&mut new_generation, event, effects, prev_generation));
        info!("Chron ingest: Advanced {} versions from previous observations, resulting in {} successors",
            num_prev_versions, new_generation.inner().len() - num_successors_before);

        prev_generation = save_and_store_successors(state, new_generation.into_inner(), event.time, event.id)?;
    }

    // Versions only make it into prev_generation after a successful observation, so if that's empty
    // it means there were zero successful observations
    if prev_generation.is_empty() {
        return Err(ChronIngestError::Conflicts(GenerationConflicts(all_conflicts)));
    }

    state.terminate_versions(ids_to_terminate,
                             format!("Failed to apply observation at {}", perceived_at))?;

    Ok(())
}

fn save_and_store_successors<EntityT: Entity>(
    state: &StateInterface,
    new_generation: Vec<((EntityT, Value, Vec<DateTime<Utc>>), Vec<i32>)>,
    start_time: DateTime<Utc>,
    from_event: i32
) -> QueryResult<Vec<(EntityT, i32)>> {
    // This is a bit of a mess... it needs to represent the new entities both as EntityT, for use
    // in the next iteration of the loop, and as AnyEntity, for use in state.save_successors
    let successor_entities: Vec<_> = new_generation.iter()
        .map(|((entity, _, _), _)| entity)
        .cloned()
        .collect();
    let any_successors = new_generation.into_iter()
        .map(|((entity, aux, observations), parents)| ((entity.into(), aux, observations), parents));
    let successor_ids = state.save_successors(any_successors, start_time, from_event)?;

    let result = successor_entities.into_iter()
        .zip(successor_ids)
        .collect();

    Ok(result)
}

fn reverse_ingest<EntityRawT: EntityRaw>(state: &StateInterface, entity_raw: &EntityRawT, perceived_at: DateTime<Utc>) -> ChronIngestResult<()> {
    info!("This is where I would run the reverse pass");

    Ok(())
}

fn observe_generation<EntityT: Entity>(
    new_generation: &mut MergedSuccessors<(EntityT, serde_json::Value, Vec<DateTime<Utc>>)>,
    versions: Vec<(Version<EntityT>, Vec<VersionLink>)>,
    entity_raw: &EntityT::Raw,
    perceived_at: DateTime<Utc>,
) -> Vec<Vec<Conflict>> {
    let mut version_conflicts = Vec::new();

    for (version, parents) in versions {
        match observe_entity(version, entity_raw, perceived_at) {
            Ok(new_version) => {
                let parent_ids = parents.into_iter()
                    .map(|parent| parent.parent_id)
                    .collect();
                new_generation.add_multi_parent_successor(parent_ids, new_version);
            }
            Err(conflicts) => {
                version_conflicts.push(conflicts);
            }
        }
    }

    return version_conflicts;
}

fn observe_entity<EntityT: Entity>(
    version: Version<EntityT>,
    entity_raw: &EntityT::Raw,
    perceived_at: DateTime<Utc>,
) -> Result<(EntityT, serde_json::Value, Vec<DateTime<Utc>>), Vec<Conflict>> {
    let mut new_entity = version.entity;
    let conflicts = new_entity.observe(entity_raw);
    if !conflicts.is_empty() {
        return Err(conflicts);
    }

    let mut observations = version.observations;
    observations.push(perceived_at);
    Ok((new_entity, version.event_aux_data, observations))
}


fn advance_generation<EntityT: Entity, EventT: Event>(
    new_generation: &mut MergedSuccessors<(EntityT, serde_json::Value, Vec<DateTime<Utc>>)>,
    event: EventT,
    effects: Vec<EventEffect>,
    prev_generation: Vec<(EntityT, i32)>,
) {
    for (prev_entity, prev_version_id) in prev_generation {
        let prev_entity_any = prev_entity.into();
        for effect in &effects {
            // This is very clone-y but I can't think of a way around that
            let new_entity = event.forward(prev_entity_any.clone(), effect.aux_data.clone())
                .try_into().expect("Event::forward returned a different entity type than it was given");
            new_generation.add_successor(prev_version_id, (new_entity, effect.aux_data.clone(), vec![]));
        }
    }
}

fn add_manual_event<EntityRawT: EntityRaw>(state: &StateInterface, entity_raw: &EntityRawT, perceived_at: DateTime<Utc>) -> ChronIngestResult<()> {
    todo!()
}