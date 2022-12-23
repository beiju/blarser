use std::collections::HashSet;
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
use partial_information::{Conflict, PartialInformationCompare};
use futures::future::join_all;
use petgraph::stable_graph::NodeIndex;
use petgraph::visit::Walker;
use serde::Deserialize;
use uuid::Uuid;

use crate::api::chronicler;
use crate::ingest::task::{DebugHistoryVersion, Ingest};
use crate::entity::{self, AnyEntity, AnyEntityRaw, Entity, EntityParseError};
use crate::events::{AnyEvent, Event, with_any_event};
use crate::ingest::GraphDebugHistory;
use crate::ingest::observation::Observation;
use crate::ingest::state::{AddedReason, EntityStateGraph, StateGraphNode};
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

#[allow(unused)]
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
    #[allow(unused)] pub hash: String,
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
                    let dt_str = record.get(1).unwrap().replace(" ", "T") + ":00";
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

pub async fn load_initial_state(start_at_time: DateTime<Utc>) -> Vec<Observation> {
    initial_state(start_at_time).collect().await
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

pub fn ingest_observation(ingest: &mut Ingest, obs: Observation, debug_history: &mut GraphDebugHistory) -> Vec<AnyEvent> {
    let obs = Arc::new(obs); // sigh
    let mut state = ingest.state.lock().unwrap();
    let graph = state.entity_graph_mut(obs.entity_type, obs.entity_id)
        .expect("Tried to ingest observation for an entity that did not previously exist. \
        This should work in the future but is not implemented yet.");

    info!("Ingesting observation for {} {} between {} and {}",
        obs.entity_type, obs.entity_id, obs.earliest_time(), obs.latest_time());

    let versions = graph.get_candidate_placements(obs.earliest_time(), obs.latest_time());
    let mut queued_for_update = versions.clone();

    let debug_key = (obs.entity_type, obs.entity_id);
    debug_history.get_mut(&debug_key).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("Start of ingest at {}", obs.perceived_at),
        time: obs.perceived_at,
        tree: graph.get_debug_tree(),
        queued_for_update: Some(queued_for_update.clone()),
        currently_updating: None,
        queued_for_delete: None,
    });

    let (successes, _failures): (Vec<_>, Vec<_>) = versions.into_iter()
        .map(|version_idx| {
            info!("Running ingest on version {version_idx:?}");
            let node = graph.get_version(version_idx)
                .expect("Expected node index from get_versions_between to be valid");

            queued_for_update.remove(&version_idx);

            match &node.entity {
                AnyEntity::Sim(_) => { ingest_for_version::<entity::Sim>(graph, version_idx, obs.clone(), debug_history, &queued_for_update, obs.perceived_at) }
                AnyEntity::Player(_) => { ingest_for_version::<entity::Player>(graph, version_idx, obs.clone(), debug_history, &queued_for_update, obs.perceived_at) }
                AnyEntity::Team(_) => { ingest_for_version::<entity::Team>(graph, version_idx, obs.clone(), debug_history, &queued_for_update, obs.perceived_at) }
                AnyEntity::Game(_) => { ingest_for_version::<entity::Game>(graph, version_idx, obs.clone(), debug_history, &queued_for_update, obs.perceived_at) }
                AnyEntity::Standings(_) => { ingest_for_version::<entity::Standings>(graph, version_idx, obs.clone(), debug_history, &queued_for_update, obs.perceived_at) }
                AnyEntity::Season(_) => { ingest_for_version::<entity::Season>(graph, version_idx, obs.clone(), debug_history, &queued_for_update, obs.perceived_at) }
            }
        })
        .partition_result();

    debug_history.get_mut(&debug_key).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("End of ingest at {}", obs.perceived_at),
        time: obs.perceived_at,
        tree: graph.get_debug_tree(),
        queued_for_update: Some(queued_for_update.clone()),
        currently_updating: None,
        queued_for_delete: None,
    });

    assert!(!successes.is_empty(), "TODO Report failures");

    let prev_nodes = get_reachable_nodes(graph, graph.leafs().clone());
    let keep_nodes = get_reachable_nodes(graph, successes.iter().flatten().copied().collect());
    let delete_nodes: HashSet<_> = prev_nodes.difference(&keep_nodes).copied().collect();

    debug_history.get_mut(&debug_key).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("Before delete from ingest at {}", obs.perceived_at),
        time: obs.perceived_at,
        tree: graph.get_debug_tree(),
        queued_for_update: None,
        currently_updating: None,
        queued_for_delete: Some(delete_nodes.clone()),
    });

    for &node_idx in &delete_nodes {
        graph.remove_node(node_idx);
    }

    graph.set_leafs(successes.into_iter().flatten().collect());

    debug_history.get_mut(&debug_key).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("After delete from ingest at {}", obs.perceived_at),
        time: obs.perceived_at,
        tree: graph.get_debug_tree(),
        queued_for_update: None,
        currently_updating: None,
        queued_for_delete: Some(delete_nodes), // leave it here to make problems more obvious
    });

    Vec::new() // TODO Generate new timed events
}

fn get_reachable_nodes(graph: &EntityStateGraph, mut stack: Vec<NodeIndex>) -> HashSet<NodeIndex> {
    let mut output = HashSet::new();
    while let Some(node_idx) = stack.pop() {
        let mut parent_walker = graph.graph.parents(node_idx);
        output.insert(node_idx);

        // I think this can stop early if it hits a node that was observed before this ingest
        // started, but that will require more bookkeeping
        while let Some((_, parent_idx)) = parent_walker.walk_next(&graph.graph) {
            stack.push(parent_idx);
        }
    }
    output
}

// Within this function, the "old" and "new" prefixes refer to two parallel subtrees. "old" already
// exists and "new" is being built
fn ingest_changed_entity<EntityT, EventT>(
    graph: &mut EntityStateGraph,
    old_child_idx: NodeIndex,
    new_child_idx: NodeIndex,
    debug_history: &mut GraphDebugHistory,
    queued_for_update: &HashSet<NodeIndex>,
    debug_time: DateTime<Utc>,
) where EntityT: Entity + PartialInformationCompare + 'static,
        AnyEntity: TryInto<EntityT>,
        <AnyEntity as TryInto<EntityT>>::Error: Debug,
        for<'a> &'a AnyEntityRaw: TryInto<&'a EntityT::Raw>,
        for<'a> <&'a AnyEntityRaw as TryInto<&'a EntityT::Raw>>::Error: Debug,
        EventT: Event,
        for<'a> &'a AnyEvent: TryInto<&'a EventT>,
        for<'a> <&'a AnyEvent as TryInto<&'a EventT>>::Error: Debug {
    let event_arc = graph.get_version(old_child_idx)
        .expect("Expected node index supplied to ingest_changed_event to be valid")
        .event.clone();

    let event: &EventT = event_arc.as_ref().try_into()
        .expect("This coercion should always succeed");

    let mut all_parents_had_conflicts = true; // starts as vacuous truth
    let mut version_conflicts = Vec::new();
    let mut parent_walker = graph.graph.parents(old_child_idx);
    while let Some((old_edge_idx, old_parent_idx)) = parent_walker.walk_next(&graph.graph) {
        let old_extrapolated = graph.graph.edge_weight(old_edge_idx)
            .expect("This should always be a valid edge index");

        let new_child_node = graph.get_version(new_child_idx)
            .expect("Come on I literally just added this node");

        let old_parent_node = graph.get_version(old_parent_idx)
            .expect("This should always be a valid edge index");

        let mut new_extrapolated = old_extrapolated.clone();
        let mut new_parent = new_child_node.entity.clone();
        event.reverse(&old_parent_node.entity, &mut new_extrapolated, &mut new_parent);

        { // Debug stuff
            // Need to call get_version again because of lifetimes
            let new_child = &graph.get_version(new_child_idx)
                .expect("Come on I literally just added this node")
                .entity;

            let mut debug_graph = graph.clone();
            debug_graph.add_edge(old_parent_idx, new_child_idx, new_extrapolated.clone());
            debug_history.get_mut(&(new_child.entity_type(), new_child.id()))
                .expect("This entity should already be in debug_history")
                .versions
                .push(DebugHistoryVersion {
                    event_human_name: format!("After adding parent {old_parent_idx:?} to {}", new_child.description()),
                    time: debug_time,
                    tree: debug_graph.get_debug_tree(),
                    queued_for_update: Some(queued_for_update.clone()),
                    currently_updating: Some(old_child_idx),
                    queued_for_delete: None,
                });
        }

        // Never recurse past an observed node, but do use our reconstructed parent to check that
        // this is working
        let new_parent_idx = if let Some(obs) = &old_parent_node.observed {
            info!("Parent has been observed; checking compatibility");
            let raw: &EntityT::Raw = (&obs.entity_raw).try_into()
                .expect("Mismatched entity types");
            let mut new_parent: EntityT = new_parent.try_into()
                .expect("I just added this one");
            let conflicts = new_parent.observe(raw);
            if conflicts.is_empty() {
                all_parents_had_conflicts = false;
                graph.add_edge(old_parent_idx, new_child_idx, new_extrapolated.clone());
            } else {
                version_conflicts.push(conflicts);
                todo!("Early exit")
            }
            old_parent_idx
        } else {
            info!("Parent has not been observed; propagating changes");
            all_parents_had_conflicts = false;
            if &old_parent_node.entity != &new_parent {
                info!("Saving changes and recursing");
                // Then a change was made and we need to save it to the graph and then recurse
                let new_parent_idx = graph.add_child_disconnected(new_parent.into(), old_parent_node.event.clone(), AddedReason::RefinedFromObservation);
                graph.add_edge(new_parent_idx, new_child_idx, new_extrapolated.clone());
                // Recurse with the different event type
                let child_event_arc = &graph.get_version(old_parent_idx)
                    .expect("Expected node index supplied to ingest_changed_event to be valid")
                    .event;
                with_any_event!(child_event_arc.as_ref(), |_: EventT| {
                    ingest_changed_entity::<EntityT, EventT>(graph, old_parent_idx, new_parent_idx, debug_history, queued_for_update, debug_time)
                });

                new_parent_idx
            } else {
                info!("No changes to save");
                graph.add_edge(old_parent_idx, new_child_idx, new_extrapolated.clone());
                old_parent_idx
            }
        };

        let new_parent_node = graph.get_version(new_parent_idx)
            .expect("This should always be a valid node index");
        let new_child_node = &graph.get_version(new_child_idx)
            .expect("This should always be a valid node index");

        // Test reconstructing child from parent, now that parent should be appropriately modified
        assert_eq!(&event.forward(&new_parent_node.entity, &new_extrapolated), &new_child_node.entity);
    }

    if all_parents_had_conflicts {
        // oh no!
        panic!("All parents had conflicts: \n{version_conflicts:?}");
    }
}

fn ingest_for_version<EntityT>(
    graph: &mut EntityStateGraph,
    entity_idx: NodeIndex,
    obs: Arc<Observation>,
    debug_history: &mut GraphDebugHistory,
    queued_for_update: &HashSet<NodeIndex>,
    debug_time: DateTime<Utc>,
) -> Result<Vec<NodeIndex>, Vec<Conflict>>
// Disgustang
    where EntityT: Entity + PartialInformationCompare + Into<AnyEntity> + 'static,
          AnyEntity: TryInto<EntityT>,
          <AnyEntity as TryInto<EntityT>>::Error: Debug,
          for<'a> &'a AnyEntity: TryInto<&'a EntityT>,
          for<'a> &'a AnyEntityRaw: TryInto<&'a EntityT::Raw>,
          for<'a> <&'a AnyEntity as TryInto<&'a EntityT>>::Error: Debug,
          for<'a> <&'a AnyEntityRaw as TryInto<&'a <EntityT as PartialInformationCompare>::Raw>>::Error: Debug {
    debug_history.get_mut(&(obs.entity_type, obs.entity_id)).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("Updating {:?} during ingest at {}", entity_idx, obs.perceived_at),
        time: obs.perceived_at,
        tree: graph.get_debug_tree(),
        queued_for_update: Some(queued_for_update.clone()),
        currently_updating: Some(entity_idx),
        queued_for_delete: None,
    });

    let event = &graph.get_version(entity_idx)
        .expect("Expected node index supplied to ingest_for_version to be valid")
        .event;
    with_any_event!(event.as_ref(), |_: EventT| {
        ingest_for_event::<EntityT, EventT>(graph, entity_idx, obs, debug_history, queued_for_update, debug_time)
    })
}

fn ingest_for_event<EntityT, EventT>(
    graph: &mut EntityStateGraph,
    entity_idx: NodeIndex,
    obs: Arc<Observation>,
    debug_history: &mut GraphDebugHistory,
    queued_for_update: &HashSet<NodeIndex>,
    debug_time: DateTime<Utc>,
) -> Result<Vec<NodeIndex>, Vec<Conflict>>
// Disgustang
    where EntityT: Entity + PartialInformationCompare + Into<AnyEntity> + 'static,
          AnyEntity: TryInto<EntityT>,
          <AnyEntity as TryInto<EntityT>>::Error: Debug,
          for<'a> &'a AnyEntity: TryInto<&'a EntityT>,
          for<'a> <&'a AnyEntity as TryInto<&'a EntityT>>::Error: Debug,
          for<'a> &'a AnyEntityRaw: TryInto<&'a EntityT::Raw>,
          for<'a> <&'a AnyEntityRaw as TryInto<&'a EntityT::Raw>>::Error: Debug,
          EventT: Event,
          for<'a> &'a AnyEvent: TryInto<&'a EventT>,
          for<'a> <&'a AnyEvent as TryInto<&'a EventT>>::Error: Debug {
    let node = graph.get_version(entity_idx)
        .expect("Expected node index supplied to ingest_for_event to be valid");

    let entity: &EntityT = (&node.entity).try_into()
        .expect("This coercion should always succeed");

    let mut new_entity = entity.clone();
    let raw: &EntityT::Raw = (&obs.entity_raw).try_into()
        .expect("TODO: use Result to report this error");
    let conflicts = new_entity.observe(raw);
    if !conflicts.is_empty() {
        return Err(conflicts);
    }

    let entity_was_changed = &new_entity != entity;
    info!("entity was {}changed", if entity_was_changed { "" } else { "not "});

    let new_entity_idx = if entity_was_changed {
        let new_entity_idx = graph.add_observed_child_disconnected(
            new_entity.into(),
            node.event.clone(),
            AddedReason::RefinedFromObservation,
            obs.clone(),
        );
        ingest_changed_entity::<EntityT, _>(
            graph,
            entity_idx,
            new_entity_idx,
            debug_history,
            queued_for_update,
            debug_time,
        );

        new_entity_idx
    } else {
        entity_idx
    };

    debug_history.get_mut(&(obs.entity_type, obs.entity_id)).unwrap().versions.push(DebugHistoryVersion {
        event_human_name: format!("After updating entity and parents {}", obs.perceived_at),
        time: obs.perceived_at,
        tree: graph.get_debug_tree(),
        queued_for_update: None,
        currently_updating: None,
        queued_for_delete: None,
    });

    let mut generation = vec![(entity_idx, new_entity_idx)];
    loop {
        // generations loop
        info!("Forward pass: walking next generation");
        let mut next_generation = Vec::new();
        for &(old_entity_idx, new_entity_idx) in &generation {
            info!("Forward pass: walking children of {old_entity_idx:?}");
            let mut child_walker = graph.graph.children(old_entity_idx);
            while let Some((edge_idx, old_child_idx)) = child_walker.walk_next(&graph.graph) {
                info!("Forward pass: walking child {old_child_idx:?}");
                // Gotta re-fetch because of borrowing rules
                let old_entity_node = graph.graph.node_weight(old_entity_idx)
                    .expect("Must exist");
                let new_entity_node = graph.graph.node_weight(new_entity_idx)
                    .expect("Must exist");
                let extrapolated = graph.graph.edge_weight(edge_idx)
                    .expect("Must exist");
                let old_child_node = graph.graph.node_weight(old_child_idx)
                    .expect("Must exist");
                let new_child_unobserved = old_child_node.event.forward(&old_entity_node.entity, &extrapolated);
                info!("Old parent: {:?}", old_entity_node.entity);
                info!("New parent: {:?}", new_entity_node.entity);
                info!("Applying Event: {:?}", old_child_node.event);
                info!("Old child: {:?}", old_child_node.entity);
                info!("New child: {:?}", new_child_unobserved);
                // for debugging, placed here because of borrow rules
                let event_description = old_child_node.event.to_string();

                // Unfortunately, observations are not totally ordered, so sometimes we need to
                // reapply an observation in the forward pass
                let (new_child, observed) = if let Some(old_obs) = &old_child_node.observed {
                    info!("Reapplying observation for {old_child_idx:?}");
                    let raw: &EntityT::Raw = (&old_obs.entity_raw).try_into()
                        .expect("This graph has inconsistent entity types");
                    let unobserved: &EntityT = (&new_child_unobserved).try_into()
                        .expect("Conflicting entity types");
                    let mut new_child_entity = unobserved.clone();

                    let conflicts = new_child_entity.observe(raw);
                    if !conflicts.is_empty() {
                        return Err(conflicts)
                    }

                    if &new_child_entity != unobserved {
                        // TODO Apply to parent
                    }

                    (new_child_entity.into(), Some(old_obs.clone()))
                } else {
                    (new_child_unobserved, None)
                };

                let (_, new_child_idx) = graph.graph.add_child(new_entity_idx, extrapolated.clone(), StateGraphNode {
                    entity: new_child,
                    event: old_child_node.event.clone(),
                    observed,
                    added_reason: AddedReason::DescendantOfObservedNode,
                });
                next_generation.push((old_child_idx, new_child_idx));

                debug_history.get_mut(&(obs.entity_type, obs.entity_id)).unwrap().versions.push(DebugHistoryVersion {
                    event_human_name: format!("After forward pass step for {event_description} at {}", obs.perceived_at),
                    time: obs.perceived_at,
                    tree: graph.get_debug_tree(),
                    queued_for_update: None,
                    currently_updating: None,
                    queued_for_delete: None,
                });
            }
        }
        if next_generation.is_empty() {
            return Ok(generation.into_iter().map(|(_old, new)| new).collect());
        }
        generation = next_generation;
    }
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