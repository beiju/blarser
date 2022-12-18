use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::iter;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use futures::{stream, Stream, StreamExt};
use itertools::Itertools;
use rocket::info;
use thiserror::Error;
use partial_information::{Conflict, PartialInformationCompare};
use futures::future::join_all;
use futures::stream::Peekable;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::Walker;
use serde::Deserialize;
use uuid::Uuid;

use crate::api::chronicler;
use crate::ingest::task::{DebugHistoryVersion, DebugTree, Ingest};
use crate::entity::{self, AnyEntity, AnyEntityRaw, Entity, EntityParseError, EntityRaw};
use crate::events::{self, AnyEvent, Event};
use crate::ingest::GraphDebugHistory;
use crate::ingest::observation::Observation;
use crate::ingest::state::EntityStateGraph;
use crate::state::EntityType;
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
            .map(|s| s.as_mut().peek());
        let (chosen_stream, _) = join_all(peeks).await.into_iter()
            .flatten()
            .enumerate()
            .min_by_key(|(_, obs)| obs.latest_time())
            .expect("This should never be None");

        Some((streams[chosen_stream].next().await.unwrap(), streams))
    })
}

#[derive(Deserialize, Debug)]
struct CsvRow {
    pub entity_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub hash: String,
    pub data: serde_json::Value,
}

pub fn chron_updates_hardcoded(start_at_time: DateTime<Utc>) -> impl Iterator<Item=Observation> {
    // So much of this is just making the type system happy
    let iters = chronicler::ENDPOINT_NAMES.into_iter()
        .chain(iter::once("game"))
        .flat_map(move |entity_type| {
            let path = Path::new("blarser").join("data").join(entity_type.to_owned() + ".csv");
            let file = File::open(path).ok()?;
            let rdr = csv::Reader::from_reader(BufReader::new(file));

            let iter = rdr.into_records()
                .filter_map(move |result| {
                    let record = result.expect("Reading CSV row failed");
                    let dt_str = (record.get(1).unwrap().replace(" ", "T") + ":00");
                    //dbg!(&dt_str);
                    let row = CsvRow {
                        entity_id: Uuid::try_parse(record.get(0).unwrap()).unwrap(),
                        timestamp: DateTime::from(DateTime::parse_from_rfc3339(&dt_str).unwrap()),
                        hash: record.get(2).unwrap().to_string(),
                        data: serde_json::from_str(&record.get(3).unwrap())
                            .expect("JSON parse from CSV failed"),
                    };
                    if row.timestamp < start_at_time { return None; }
                    let entity_type = entity_type.try_into().unwrap();
                    Some(Observation {
                        perceived_at: row.timestamp,
                        entity_type,
                        entity_id: row.entity_id,
                        entity_raw: AnyEntityRaw::from_json(entity_type, row.data).unwrap(),
                    })
                });

            Some(iter.peekable())
        })
        .collect_vec();

    info!("Got {} iterators", iters.len());

    itertools::unfold(iters, |iters| {
        let peeks = iters.iter_mut()
            .map(|s| s.peek())
            .collect_vec();
        let (chosen_stream, _) = peeks.into_iter()
            .flatten()
            .enumerate()
            .min_by_key(|(_, obs)| obs.latest_time())
            .expect("This should never be None");

        Some(iters[chosen_stream].next().unwrap())
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

pub fn ingest_observation(ingest: &mut Ingest, obs: Observation, debug_history: &mut GraphDebugHistory) -> Vec<AnyEvent> {
    let mut state = ingest.state.lock().unwrap();
    let mut graph = state.entity_graph_mut(obs.entity_type, obs.entity_id)
        .expect("Tried to ingest observation for an entity that did not previously exist. \
        This should work in the future but is not implemented yet.");

    info!("Ingesting observation for {} {} between {} and {}",
        obs.entity_type, obs.entity_id, obs.earliest_time(), obs.latest_time());

    let versions = graph.get_versions_between(obs.earliest_time(), obs.latest_time());
    //dbg!(&versions);

    let mut debug_tree = graph.get_debug_tree();
    for version in &versions {
        debug_tree.data.get_mut(version).unwrap().is_scheduled_for_update = true;
    }

    debug_history.get_mut(&(obs.entity_type, obs.entity_id)).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("Start of ingest at {}", obs.perceived_at),
        time: obs.perceived_at,
        value: debug_tree.clone(),
    });

    let (successes, _failures): (Vec<_>, Vec<_>) = versions.into_iter()
        .map(|version_idx| {
            let (version, event) = graph.get_version(version_idx)
                .expect("Expected node index from get_versions_between to be valid");

            //dbg!(&version);
            //dbg!(&event);

            // Round trip through the version enum to please the borrow checker
            // TODO: Is this still required after refactoring?
            let entity_type = match version {
                AnyEntity::Sim(_) => { EntityType::Sim }
                AnyEntity::Player(_) => { EntityType::Player }
                AnyEntity::Team(_) => { EntityType::Team }
                AnyEntity::Game(_) => { EntityType::Game }
                AnyEntity::Standings(_) => { EntityType::Standings }
                AnyEntity::Season(_) => { EntityType::Season }
            };

            match entity_type {
                EntityType::Sim => { ingest_for_version::<entity::Sim>(graph, version_idx, &obs, debug_history, &mut debug_tree, obs.perceived_at) }
                EntityType::Player => { ingest_for_version::<entity::Player>(graph, version_idx, &obs, debug_history, &mut debug_tree, obs.perceived_at) }
                EntityType::Team => { ingest_for_version::<entity::Team>(graph, version_idx, &obs, debug_history, &mut debug_tree, obs.perceived_at) }
                EntityType::Game => { ingest_for_version::<entity::Game>(graph, version_idx, &obs, debug_history, &mut debug_tree, obs.perceived_at) }
                EntityType::Standings => { ingest_for_version::<entity::Standings>(graph, version_idx, &obs, debug_history, &mut debug_tree, obs.perceived_at) }
                EntityType::Season => { ingest_for_version::<entity::Season>(graph, version_idx, &obs, debug_history, &mut debug_tree, obs.clone().perceived_at) }
            }
        })
        .partition_result();

    debug_history.get_mut(&(obs.entity_type, obs.entity_id)).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("End of ingest at {}", obs.perceived_at),
        time: obs.perceived_at,
        value: debug_tree.clone(),
    });

    assert!(!successes.is_empty(), "TODO Report failures");

    Vec::new() // TODO Generate new timed events
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

fn ingest_changed_event<EventT>(
    graph: &mut EntityStateGraph,
    event: Arc<EventT>,
    existing_version_idx: NodeIndex,
    newly_added_version_idx: NodeIndex,
    debug_history: &mut GraphDebugHistory,
    debug_tree: &DebugTree,
    debug_time: DateTime<Utc>,
) where EventT: Event {
    // Gather data for debug here, because lifetimes
    let (modified_child, _) = graph.get_version(newly_added_version_idx)
        .expect("Come on I literally just added this node");

    let history_key = (modified_child.entity_type(), modified_child.id());
    let child_desc = modified_child.to_string();

    let mut all_parents_had_conflicts = true; // starts as vacuous truth
    let mut version_conflicts = Vec::new();
    let mut parent_walker = graph.graph.parents(existing_version_idx);
    while let Some((edge_idx, parent_idx)) = parent_walker.walk_next(&graph.graph) {
        let old_extrapolated = graph.graph.edge_weight(edge_idx)
            .expect("This should always be a valid edge index");

        let new_edge_idx = graph.add_edge(parent_idx, newly_added_version_idx, extrapolated.clone());
        debug_history.get_mut(&history_key)
            .expect("This entity should already be in debug_history")
            .versions
            .push(DebugHistoryVersion {
                event_human_name: format!("After adding parent {parent_idx:?} for {child_desc}"),
                time: debug_time,
                value: graph.get_debug_tree(),
            });

        let (parent, parent_event) = graph.get_version(parent_idx)
            .expect("This should always be a valid node index");
        let mut new_parent = parent.clone();
        let parent_event = parent_event.clone();
        let (modified_child, _) = graph.get_version(newly_added_version_idx)
            .expect("Come on I literally just added this node");
        let new_extrapolated = event.fill_extrapolated(modified_child, &extrapolated);
        let conflicts = event.forward(modified_child, &mut extrapolated, &mut new_parent);

        if !conflicts.is_empty() {
            version_conflicts.push(conflicts);
        } else {
            all_parents_had_conflicts = false;
            if parent != &new_parent {
                // Then a change was made and we need to save it to the graph and then recurse
                let new_parent_idx = graph.add_child_disconnected(new_parent.into(), event.clone());
                // Re-target the child-to-parent link we just added to point to the new parent
                graph.remove_edge(new_edge_idx);
                graph.add_edge(new_parent_idx, newly_added_version_idx, extrapolated);
                // Recurse
                ingest_changed_event(graph, parent_event, parent_idx, new_parent_idx, debug_history, debug_tree, debug_time);
            }
        }
    }

    if all_parents_had_conflicts {
        // oh no!
        panic!("All parents had conflicts: \n{version_conflicts:?}");
    }
}

fn ingest_for_version<EntityT>(
    graph: &mut EntityStateGraph,
    entity_idx: NodeIndex,
    obs: &Observation,
    debug_history: &mut GraphDebugHistory,
    debug_tree: &mut DebugTree,
    debug_time: DateTime<Utc>,
) -> Result<(), Vec<Conflict>>
// Disgustang
    where EntityT: Entity + PartialInformationCompare + Into<AnyEntity>,
          for<'a> &'a AnyEntity: TryInto<&'a EntityT>,
          for<'a> &'a AnyEntityRaw: TryInto<&'a EntityT::Raw>,
          for<'a> <&'a AnyEntity as TryInto<&'a EntityT>>::Error: Debug,
          for<'a> <&'a AnyEntityRaw as TryInto<&'a <EntityT as PartialInformationCompare>::Raw>>::Error: Debug {
    debug_tree.data.get_mut(&entity_idx).unwrap().is_updating = true;

    debug_history.get_mut(&(obs.entity_type, obs.entity_id)).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("Updating {:?} during ingest at {}", entity_idx, obs.perceived_at),
        time: obs.perceived_at,
        value: debug_tree.clone(),
    });

    debug_tree.data.get_mut(&entity_idx).unwrap().is_updating = false;
    debug_tree.data.get_mut(&entity_idx).unwrap().is_scheduled_for_update = false;

    let (entity, event) = graph.get_version(entity_idx)
        .expect("Expected node index supplied to ingest_for_version to be valid");

    let entity: &EntityT = entity.try_into()
        .expect("This coercion should always succeed");

    // TODO Can I do Arc<AnyEvent> => Arc<EventT> for a specific EventT?
    match event.as_ref() {
        AnyEvent::Start(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::EarlseasonStart(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::LetsGo(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::PlayBall(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::TogglePerforming(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::HalfInning(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::StormWarning(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::BatterUp(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::Strike(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::Ball(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::FoulBall(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::Out(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::Hit(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::HomeRun(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::StolenBase(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::Walk(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
        AnyEvent::CaughtStealing(e) => ingest_for_event(graph, entity_idx, entity, e, obs, debug_history, debug_tree, debug_time),
    }
}

fn ingest_for_event<EntityT, EventT>(
    graph: &mut EntityStateGraph,
    entity_idx: NodeIndex,
    entity: &EntityT,
    event: &Arc<EventT>,
    obs: &Observation,
    debug_history: &mut GraphDebugHistory,
    debug_tree: &mut DebugTree,
    debug_time: DateTime<Utc>,
) -> Result<(), Vec<Conflict>>
// Disgustang
    where EntityT: Entity + PartialInformationCompare + Into<AnyEntity>,
          for<'a> &'a AnyEntityRaw: TryInto<&'a EntityT::Raw>,
          for<'a> <&'a AnyEntityRaw as TryInto<&'a <EntityT as PartialInformationCompare>::Raw>>::Error: Debug,
          EventT: Event {
    let event = event.clone();

    let mut new_entity = entity.clone();
    let raw: &EntityT::Raw = (&obs.entity_raw).try_into()
        .expect("TODO: use Result to report this error");
    let conflicts = new_entity.observe(raw);
    if !conflicts.is_empty() {
        return Err(conflicts);
    }
    debug_tree.data.get_mut(&entity_idx).unwrap().is_observed = true;

    let entity_was_changed = &new_entity != entity;

    if entity_was_changed {
        let new_entity_idx = graph.add_child_disconnected(new_entity.into(), event.clone());
        ingest_changed_event(graph, event, entity_idx, new_entity_idx, debug_history, &debug_tree, debug_time);
    }

    // todo!("Forward pass");

    // todo!("Remove every element that's not part of the new tree, perhaps using some kind of mark-and-sweep-like procedure")

    Ok(())
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