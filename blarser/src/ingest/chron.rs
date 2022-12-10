use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::{Debug, Display, Formatter};
use std::iter;
use std::pin::Pin;
use chrono::{DateTime, SecondsFormat, Utc};
use diesel::QueryResult;
use futures::{pin_mut, stream, Stream, StreamExt};
use itertools::{EitherOrBoth, Itertools};
use rocket::{info, warn};
use serde_json::Value;
use thiserror::Error;
use partial_information::{Conflict, PartialInformationCompare};
use fed::FedEvent;
use futures::future::join_all;
use futures::stream::Peekable;
use petgraph::stable_graph::NodeIndex;
use stream_kmerge::kmerge_by_key;
use uuid::Uuid;

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::Ingest;
use crate::entity::{self, AnyEntity, Entity, EntityParseError, EntityRaw};
use crate::events::{self, AnyEvent};
use crate::ingest::{self, observation::Observation, StateGraph};
use crate::state::{EntityType, EventEffect, MergedSuccessors, NewVersion, StateInterface, Version, VersionLink};
// use crate::{with_any_entity_raw, with_any_event};
// use crate::events::Event;

fn initial_state(start_at_time: DateTime<Utc>) -> impl Stream<Item=Observation> {
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

pub fn chron_updates(start_at_time: DateTime<Utc>) -> impl Stream<Item=Observation> {
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
        }))
        .map(|s| Box::pin(s.peekable()))
        .collect_vec();

    stream::unfold(streams, |mut streams| async {
        let peeks = streams.iter_mut()
            .map(|mut s| s.as_mut().peek());
        let (chosen_stream, _) = join_all(peeks).await.into_iter()
            .flatten()
            .enumerate()
            .min_by_key(|(_, obs)| obs.latest_time())
            .expect("This should never be None");

        Some((streams[chosen_stream].next().await.unwrap(), streams))
    })
}

pub async fn load_initial_state(ingest: &Ingest, start_at_time: DateTime<Utc>) -> Vec<Observation> {
    let initial_versions: Vec<_> = initial_state(start_at_time).collect().await;

    // ingest.run(move |mut state| {
    //     state.add_initial_versions(start_time_parsed, initial_versions.into_iter())
    // }).await
    //     .expect("Failed to save initial versions");
    initial_versions
}

pub(crate) struct ObservationStreamWithCursor<'s, StreamT: Stream<Item=Observation>> {
    stream: Pin<&'s mut Peekable<StreamT>>,
}

impl<'s, StreamT: Stream<Item=Observation>> ObservationStreamWithCursor<'s, StreamT> {
    pub fn new(stream: Pin<&'s mut Peekable<StreamT>>) -> Self {
        Self { stream }
    }

    pub async fn next_before(&mut self, limit: DateTime<Utc>) -> Option<Observation> {
        let Some(next_item) = self.stream.as_mut().peek().await else {
            return None;
        };

        if next_item.latest_time() < limit {
            self.stream.next().await
        } else {
            None
        }
    }

    pub async fn next_cursor(&mut self) -> Option<DateTime<Utc>> {
        self.stream.as_mut().peek().await.map(|obs| obs.latest_time())
    }
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

pub type ChronIngestResult<T> = Result<T, ChronIngestError>;

pub fn ingest_observation(ingest: &mut Ingest, obs: Observation) -> Vec<AnyEvent> {
    let mut state = ingest.state.lock().unwrap();

    // **OUTDATED** CONCEPT:
    // 0. Initialize a new graph
    // 1. Get every stored version of this entity between earliest_time and latest_time
    // 2. For each version
    //    a. Try to apply the observation to the entity. If it fails, bail early. Otherwise, the
    //       entity is now equal to the observation.
    //    b. For each parent and its associated Effect:
    //       i. Try to run the backwards pass given the two entity versions and the effect that
    //          links them. If the application fails, bail. If it succeeds, update the Extrapolated
    //          if necessary and then add both versions and the connecting event to a new graph. If the success also changed the parent event, get its
    //       parent and repeat the process until an ancestor doesn't change. This is going to need
    //       to traverse branches and I haven't thought that out fully yet.
    //    c. Once you're done with ancestors, re-generate the descendants just using the Effects. No
    //       need to incorporate the entities here -- they will not have any useful information.
    //    d. Return the new subtree
    // 3. If all placements failed, display an error to the user. Otherwise, find the tallest tree
    //    among all the successful trees. Make each tree equally tall by plucking ancestors directly
    //    from the state without modifying them.
    // 4. Merge the subtrees. This involves comparing versions and merging them when they're equal,
    //    but keeping the edges intact. (I just realized this may end up with multiple edges between
    //    the same two nodes, which the graph library doesn't support. Uh oh.)
    // 5. The subtrees should all converge at the same root, because it was unmodified. Graft that
    //    root back onto the tree, in the same place it came from, replacing what was there before.
    // 6. Profit
    info!("Ingesting observation for {} {} at {}", obs.entity_type, obs.entity_id, obs.perceived_at);

    let versions = state.get_versions_between(obs.entity_type, obs.entity_id, obs.earliest_time(), obs.latest_time())
        .expect("Asked for versions for entity that does not exist");
    dbg!(&versions);

    let (successes, _failures): (Vec<_>, Vec<_>) = versions.into_iter()
        .map(|version_idx| {
            let (version, event) = state.version(version_idx)
                .expect("Expected node index from get_versions_between to be valid");

            dbg!(&version);
            dbg!(&event);

            // Round trip through the version enum to please the borrow checker
            let entity_type = match version {
                AnyEntity::Sim(_) => { EntityType::Sim }
                AnyEntity::Player(_) => { EntityType::Player }
                AnyEntity::Team(_) => { EntityType::Team }
                AnyEntity::Game(_) => { EntityType::Game }
                AnyEntity::Standings(_) => { EntityType::Standings }
                AnyEntity::Season(_) => { EntityType::Season }
            };

            match entity_type {
                EntityType::Sim => { ingest_for_version::<entity::Sim>(&mut state, version_idx, &obs) }
                EntityType::Player => { ingest_for_version::<entity::Player>(&mut state, version_idx, &obs) }
                EntityType::Team => { ingest_for_version::<entity::Team>(&mut state, version_idx, &obs) }
                EntityType::Game => { ingest_for_version::<entity::Game>(&mut state, version_idx, &obs) }
                EntityType::Standings => { ingest_for_version::<entity::Standings>(&mut state, version_idx, &obs) }
                EntityType::Season => { ingest_for_version::<entity::Season>(&mut state, version_idx, &obs) }
            }
        })
        .partition_result();

    assert!(!successes.is_empty(), "TODO Report failures");

    todo!()
}

struct Strand {
    original: AnyEntity,
    // Goes in reverse chronological order, so newest -> oldest
    backwards: Vec<AnyEntity>,
    // Goes in chronological order, so oldest -> newest
    forwards: Vec<AnyEntity>,
}

impl Strand {
    pub fn new(entity: AnyEntity) -> Self {
        Self {
            original: entity,
            backwards: Default::default(),
            forwards: Default::default(),
        }
    }
}

fn backwards_pass<EntityT>(state: &mut StateGraph, entity_idx: NodeIndex, mut current_entity: EntityT) -> Vec<AnyEntity>
    where EntityT: Entity + PartialInformationCompare {
    todo!()
}

fn ingest_for_version<EntityT>(state: &mut StateGraph, entity_idx: NodeIndex, obs: &Observation) -> Result<Strand, Vec<Conflict>>
    // Disgustang
    where EntityT: Entity + PartialInformationCompare, for<'a> &'a AnyEntity: TryInto<&'a EntityT>,
          for<'a> <&'a AnyEntity as TryInto<&'a EntityT>>::Error: Debug {

    let (entity, _) = state.version(entity_idx)
        .expect("Expected node index supplied to ingest_for_version to be valid");

    let entity: &EntityT = entity.try_into()
        .expect("This coercion should always succeed");

    let mut new_entity = entity.clone();
    let raw: EntityT::Raw = serde_json::from_value(obs.entity_json.clone())
        .expect("TODO: use Result to report this error");
    let conflicts = new_entity.observe(&raw);
    if !conflicts.is_empty() {
        return Err(conflicts);
    }

    let backwards = if &new_entity != entity {
        backwards_pass(state, entity_idx, new_entity)
    } else {
        Vec::new()
    };

    todo!()
}

// fn forward_ingest<EntityRawT: EntityRaw>(state: &StateInterface, entity_raw: &EntityRawT, perceived_at: DateTime<Utc>) -> ChronIngestResult<()> {
//     let earliest_time = entity_raw.earliest_time(perceived_at);
//     let latest_time = entity_raw.latest_time(perceived_at);
//     let events = state.get_events_for_versions_after(entity_raw, earliest_time)?;
//     let generations = state.get_versions_for_entity_raw_between(entity_raw, earliest_time, latest_time)?;
//
//     info!("Chron ingest: Applying observation to {} {} between {} and {}. {} generations, {} events",
//         EntityRawT::name(), entity_raw.id(), earliest_time, latest_time, generations.len(), events.len());
//
//     // The generation at the end of the window should be terminated after the whole process
//     let ids_to_terminate: Vec<_> = generations.last()
//         .expect("Chron ingest found zero generations in the observation window")
//         .1.iter()
//         .map(|(version, _)| version.id)
//         .collect();
//
//     let mut prev_generation = Vec::new();
//     let mut all_conflicts = Vec::new();
//     for either_or_both in events.into_iter().zip_longest(generations) {
//         let ((event, effects), existing_versions) = match either_or_both {
//             EitherOrBoth::Both((event, effects), (event_id, versions)) => {
//                 assert_eq!(event_id, event.id, "Generation's event_id did not match expected event");
//                 ((event, effects), Some(versions))
//             }
//             EitherOrBoth::Left(event_effects) => {
//                 (event_effects, None)
//             }
//             EitherOrBoth::Right(_) => {
//                 panic!("Got a generation without the corresponding event");
//             }
//         };
//
//         let mut new_generation = MergedSuccessors::new();
//
//         if let Some(versions) = existing_versions {
//             let version_time = versions.first().expect("Empty generation").0.start_time;
//             let num_versions = versions.len();
//             let version_conflicts = observe_generation(&mut new_generation, versions, entity_raw, perceived_at);
//             info!("Chron ingest: Generation at {} with {} versions observed, resulting in {} successors and {} conflicts",
//             version_time, num_versions, new_generation.inner().len(), version_conflicts.len());
//
//             all_conflicts.push(GenerationConflict {
//                 start_time: version_time,
//                 event_name: event.event.type_name(),
//                 version_conflicts,
//             });
//         }
//
//         let num_prev_versions = prev_generation.len();
//         let num_successors_before = new_generation.inner().len();
//         with_any_event!(event.event, event => advance_generation(&mut new_generation, event, effects, prev_generation));
//         info!("Chron ingest: Advanced {} versions from previous observations, resulting in {} successors",
//             num_prev_versions, new_generation.inner().len() - num_successors_before);
//
//         prev_generation = save_and_store_successors(state, new_generation.into_inner(), event.time, event.id)?;
//     }
//
//     // Versions only make it into prev_generation after a successful observation, so if that's empty
//     // it means there were zero successful observations
//     if prev_generation.is_empty() {
//         return Err(ChronIngestError::Conflicts(GenerationConflicts(all_conflicts)));
//     }
//
//     state.terminate_versions(ids_to_terminate,
//                              format!("Failed to apply observation at {}", perceived_at))?;
//
//     Ok(())
// }
//
// fn save_and_store_successors<EntityT: Entity>(
//     state: &StateInterface,
//     new_generation: Vec<((EntityT, Value, Vec<DateTime<Utc>>), Vec<i32>)>,
//     start_time: DateTime<Utc>,
//     from_event: i32
// ) -> QueryResult<Vec<(EntityT, i32)>> {
//     // This is a bit of a mess... it needs to represent the new entities both as EntityT, for use
//     // in the next iteration of the loop, and as AnyEntity, for use in state.save_successors
//     let successor_entities: Vec<_> = new_generation.iter()
//         .map(|((entity, _, _), _)| entity)
//         .cloned()
//         .collect();
//     let any_successors = new_generation.into_iter()
//         .map(|((entity, aux, observations), parents)| ((entity.into(), aux, observations), parents));
//     let successor_ids = state.save_successors(any_successors, start_time, from_event)?;
//
//     let result = successor_entities.into_iter()
//         .zip(successor_ids)
//         .collect();
//
//     Ok(result)
// }
//
// fn reverse_ingest<EntityRawT: EntityRaw>(state: &StateInterface, entity_raw: &EntityRawT, perceived_at: DateTime<Utc>) -> ChronIngestResult<()> {
//     info!("This is where I would run the reverse pass");
//
//     Ok(())
// }
//
// fn observe_generation<EntityT: Entity>(
//     new_generation: &mut MergedSuccessors<(EntityT, serde_json::Value, Vec<DateTime<Utc>>)>,
//     versions: Vec<(Version<EntityT>, Vec<VersionLink>)>,
//     entity_raw: &EntityT::Raw,
//     perceived_at: DateTime<Utc>,
// ) -> Vec<Vec<Conflict>> {
//     let mut version_conflicts = Vec::new();
//
//     for (version, parents) in versions {
//         match observe_entity(version, entity_raw, perceived_at) {
//             Ok(new_version) => {
//                 let parent_ids = parents.into_iter()
//                     .map(|parent| parent.parent_id)
//                     .collect();
//                 new_generation.add_multi_parent_successor(parent_ids, new_version);
//             }
//             Err(conflicts) => {
//                 version_conflicts.push(conflicts);
//             }
//         }
//     }
//
//     return version_conflicts;
// }
//
// fn observe_entity<EntityT: Entity>(
//     version: Version<EntityT>,
//     entity_raw: &EntityT::Raw,
//     perceived_at: DateTime<Utc>,
// ) -> Result<(EntityT, serde_json::Value, Vec<DateTime<Utc>>), Vec<Conflict>> {
//     let mut new_entity = version.entity;
//     let conflicts = new_entity.observe(entity_raw);
//     if !conflicts.is_empty() {
//         return Err(conflicts);
//     }
//
//     let mut observations = version.observations;
//     observations.push(perceived_at);
//     Ok((new_entity, version.event_aux_data, observations))
// }
//
//
// fn advance_generation<EntityT: Entity, EventT: Event>(
//     new_generation: &mut MergedSuccessors<(EntityT, serde_json::Value, Vec<DateTime<Utc>>)>,
//     event: EventT,
//     effects: Vec<EventEffect>,
//     prev_generation: Vec<(EntityT, i32)>,
// ) {
//     for (prev_entity, prev_version_id) in prev_generation {
//         let prev_entity_any = prev_entity.into();
//         for effect in &effects {
//             // This is very clone-y but I can't think of a way around that
//             let new_entity = event.forward(prev_entity_any.clone(), effect.aux_data.clone())
//                 .try_into().expect("Event::forward returned a different entity type than it was given");
//             new_generation.add_successor(prev_version_id, (new_entity, effect.aux_data.clone(), vec![]));
//         }
//     }
// }
//
// fn add_manual_event<EntityRawT: EntityRaw>(state: &StateInterface, entity_raw: &EntityRawT, perceived_at: DateTime<Utc>) -> ChronIngestResult<()> {
//     todo!()
// }