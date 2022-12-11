use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::iter;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::{error, info};
use daggy::stable_dag::{StableDag, NodeIndex};
use petgraph::visit::Walker;
use diesel::PgJsonbExpressionMethods;
use petgraph::data::DataMap;
use serde_json::json;
use uuid::Uuid;
use partial_information::PartialInformationCompare;

use crate::entity::{self, AnyEntity, Entity};
use crate::events::{Effect, AnyEvent, EarlseasonStart, Event, AnyExtrapolated, Start};
use crate::ingest::error::{IngestError, IngestResult};
use crate::ingest::{GraphDebugHistory, Observation};
use crate::ingest::task::{DebugHistoryItem, DebugHistoryVersion, DebugSubtree, DebugSubtreeNode};
use crate::state::EntityType;

pub type StateGraphNode = (AnyEntity, Arc<AnyEvent>);
pub type StateGraphEdge = AnyExtrapolated;

#[derive(Default)]
pub struct EntityStateGraph {
    pub(crate) graph: StableDag<StateGraphNode, StateGraphEdge>,
    roots: Vec<NodeIndex>,
    leafs: Vec<NodeIndex>,
}

impl EntityStateGraph {
    pub fn new(first_node: (AnyEntity, Arc<AnyEvent>)) -> Self {
        let mut s = Self {
            graph: StableDag::new(),
            roots: Vec::new(),
            leafs: Vec::new(),
        };

        let idx = s.graph.add_node(first_node);
        s.leafs.push(idx);
        s.roots.push(idx);

        s
    }

    pub fn leafs(&self) -> &Vec<NodeIndex> {
        &self.leafs
    }

    pub fn set_leafs(&mut self, leafs: Vec<NodeIndex>) -> Vec<NodeIndex> {
        std::mem::replace(&mut self.leafs, leafs)
    }

    pub fn roots(&self) -> &Vec<NodeIndex> {
        &self.roots
    }

    pub fn get_version(&self, idx: NodeIndex) -> Option<&StateGraphNode> {
        self.graph.node_weight(idx)
    }

    pub fn add_child_version(&mut self, parent_idx: NodeIndex, new_entity: AnyEntity, event: Arc<AnyEvent>, extrapolated: AnyExtrapolated) -> NodeIndex {
        let child_idx = self.graph.add_node((new_entity, event));
        self.graph.add_edge(parent_idx, child_idx, extrapolated.clone()).unwrap();
        child_idx
    }

    pub fn get_versions_between(&self, earliest: DateTime<Utc>, latest: DateTime<Utc>) -> HashSet<NodeIndex> {
        // I couldn't figure out how to do what I wanted with the built-in graph traversal helpers
        // so I made my own traversal
        let mut stack = self.leafs.clone();
        let mut visited = HashSet::new();
        let mut outputs = HashSet::new();
        while let Some(node) = stack.pop() {
            visited.insert(node);
            // Get lifetime of this node
            let (_, event) = self.graph.node_weight(node)
                .expect("Stack contained a node that was not in the graph");
            let earliest_node_time: DateTime<Utc> = event.time();
            let mut latest_node_time = None;
            let mut child_walker = self.graph.children(node);
            while let Some((_, child)) = child_walker.walk_next(&self.graph) {
                let (_, child_event) = self.graph.node_weight(child)
                    .expect("Graph gave me an invalid index");
                if let Some(prev_time) = latest_node_time.replace(child_event.time()) {
                    assert_eq!(prev_time, child_event.time(),
                               "All children of the same node must have the same time");
                }
            }
            // If this node starts later than the end of the time window, then don't bother adding
            // it or searching its children
            if earliest_node_time > latest { continue; }
            // Add children
            let mut child_walker = self.graph.children(node);
            while let Some((_, child)) = child_walker.walk_next(&self.graph) {
                if !visited.contains(&child) { stack.push(child); }
            }
            // If this node ends earlier than the beginning of the time window, don't add it
            if latest_node_time.map_or(false, |t| t < earliest) { continue; }
            outputs.insert(node);
        }

        outputs
    }
}

#[derive(Default)]
pub struct StateGraph {
    pub(crate) graphs: HashMap<(EntityType, Uuid), EntityStateGraph>,
    ids_for_type: HashMap<EntityType, Vec<Uuid>>,
}

fn insert_from_observation<EntityT: Entity + PartialInformationCompare>(vec: &mut Vec<EntityT>, raw_json: serde_json::Value) {
    let raw: EntityT::Raw = serde_json::from_value(raw_json)
        .expect("TODO handle errors");
    let entity = EntityT::from_raw(raw);
    vec.push(entity);
}

impl StateGraph {
    pub fn new() -> Self { Default::default() }

    pub fn populate(&mut self, obses: Vec<Observation>, start_time: DateTime<Utc>, history: &mut GraphDebugHistory) {
        let start_event: Arc<AnyEvent> = Arc::new(Start::new(start_time).into());
        for obs in obses {
            let entity = AnyEntity::from_raw_json(obs.entity_type, obs.entity_json.clone()) // remove the clone after finished debugging
                .map_err(|e| {
                    error!("{e} for {} {}: {}", obs.entity_type, obs.entity_id, obs.entity_json);
                    e
                })
                .expect("JSON parsing failed");

            // Unfortunately these assignments all have to be in a specific order that makes it
            // not particularly easy to tell what's going on. Gathering data for the debug view is
            // interleaved with meaningful work.
            let entity_human_name = entity.to_string();
            let entity_json = entity.to_json();
            let new_graph = EntityStateGraph::new((entity, start_event.clone()));
            let generations = vec![new_graph.roots().iter().cloned().collect()];
            let idx = *new_graph.roots().iter().exactly_one().unwrap();
            self.graphs.insert((obs.entity_type, obs.entity_id),new_graph);
            self.ids_for_type.entry(obs.entity_type).or_default().push(obs.entity_id);
            history.insert((obs.entity_type, obs.entity_id), DebugHistoryItem {
                entity_human_name,
                versions: vec![DebugHistoryVersion {
                    event_human_name: "Start".to_string(),
                    time: start_time,
                    value: DebugSubtree {
                        generations,
                        edges: Default::default(),
                        data: iter::once((idx, DebugSubtreeNode {
                            is_ambiguous: false, // can't be ambiguous at start
                            is_observed: true, // by definition
                            json: entity_json,
                        })).collect(),
                    },
                }],
            });
        }
    }

    pub fn entity_graph(&self, entity_type: EntityType, id: Uuid) -> Option<&EntityStateGraph> {
        self.graphs.get(&(entity_type, id))
    }

    pub fn entity_graph_mut(&mut self, entity_type: EntityType, id: Uuid) -> Option<&mut EntityStateGraph> {
        self.graphs.get_mut(&(entity_type, id))
    }

    pub fn get_timed_events(&self, after: DateTime<Utc>) -> Vec<AnyEvent> {
        // This function is not intended to be generic. I need to see the natural usage pattern in
        // the normal case before deciding what the general API will look like.
        let sim_graph = self.entity_graph(EntityType::Sim, Uuid::nil())
            .expect("Error: Missing sim graph");
        let sim_idx = sim_graph.leafs()
            .into_iter().exactly_one()
            .expect("There must be exactly one sim node when calling get_timed_events");
        let (entity, _) = sim_graph.get_version(*sim_idx)
            .expect("Sim was not found in graph");
        let sim: &entity::Sim = entity.try_into()
            .expect("Sim object was not Sim type");

        if sim.phase == 1 && sim.earlseason_date > after {
            vec![AnyEvent::from(EarlseasonStart::new(sim.earlseason_date))]
        } else {
            vec![]
        }
    }

    pub fn ids_for(&self, effect: &Effect) -> Vec<Uuid> {
        if let Some(id) = effect.id {
            vec![id]
        } else if let Some(d) = self.ids_for_type.get(&effect.ty) {
            d.clone()
        } else {
            Vec::new()
        }
    }

    pub fn apply_event(&mut self, event: Arc<AnyEvent>, ty: EntityType, id: Uuid, extrapolated: &AnyExtrapolated) -> IngestResult<Vec<AnyEvent>> {
        let graph = self.entity_graph_mut(ty, id)
            .expect("Tried to apply event to entity that does not exist");

        info!("Applying {event} to {ty} {id} with {extrapolated:?}");
        let new_leafs = graph.leafs().iter()
            .map(move |&entity_idx| {
                self.apply_event_to_entity(graph, event.clone(), entity_idx, extrapolated)
            })
            .collect();

        graph.set_leafs(new_leafs);

        Ok(Vec::new()) // TODO
    }

    fn apply_event_to_entity(&mut self, graph: &mut EntityStateGraph, event: Arc<AnyEvent>, entity_idx: NodeIndex, extrapolated: &AnyExtrapolated) -> NodeIndex {
        let (entity, _) = graph.get_version(entity_idx)
            .expect("Indices in State.leafs should always be valid");

        let new_entity = match event.as_ref() {
            AnyEvent::Start(e) => { e.forward(entity, extrapolated) }
            AnyEvent::EarlseasonStart(e) => { e.forward(entity, extrapolated) }
            AnyEvent::LetsGo(e) => { e.forward(entity, extrapolated) }
            AnyEvent::PlayBall(e) => { e.forward(entity, extrapolated) }
            AnyEvent::TogglePerforming(e) => { e.forward(entity, extrapolated) }
            AnyEvent::HalfInning(e) => { e.forward(entity, extrapolated) }
            AnyEvent::StormWarning(e) => { e.forward(entity, extrapolated) }
            AnyEvent::BatterUp(e) => { e.forward(entity, extrapolated) }
            AnyEvent::Strike(e) => { e.forward(entity, extrapolated) }
            AnyEvent::Ball(e) => { e.forward(entity, extrapolated) }
            AnyEvent::FoulBall(e) => { e.forward(entity, extrapolated) }
            AnyEvent::Out(e) => { e.forward(entity, extrapolated) }
            AnyEvent::Hit(e) => { e.forward(entity, extrapolated) }
            AnyEvent::HomeRun(e) => { e.forward(entity, extrapolated) }
            AnyEvent::StolenBase(e) => { e.forward(entity, extrapolated) }
        };

        graph.add_child_version(entity_idx, new_entity, event, extrapolated.clone())
    }

    fn query_entity_unique<EntityT: Entity, F, T>(&self, leaf_id: &(EntityType, Uuid), accessor: F) -> T
        where F: Fn(&EntityT) -> T,
              T: Debug + Eq,
              for<'a> &'a AnyEntity: TryInto<&'a EntityT>,
              for<'a> <&'a AnyEntity as TryInto<&'a EntityT>>::Error: Debug {
        // TODO Don't take these paramters as a tuple any more
        let graph = self.entity_graph(leaf_id.0, leaf_id.1)
            .expect("Entity not found. TODO: Make this a Result type");
        let mut result = None;
        for leaf in graph.leafs {
            let (entity, _) = graph.get_version(leaf)
                .expect("Leafs should never have an invalid index");
            let entity: &EntityT = entity.try_into()
                .expect("Corrupt graph: Leaf was not the expected type");
            let new_result = accessor(entity);
            if let Some(old_result) = &result {
                assert_eq!(old_result, &new_result,
                           "Got different results when querying entity. TODO: Make this a Result type");
            }
            result = Some(new_result)
        }

        result.expect("Leafs array for entity is empty")
    }

    pub fn query_game_unique<F, T>(&self, id: Uuid, accessor: F) -> T
        where F: Fn(&entity::Game) -> T, T: Debug + Eq {
        self.query_entity_unique::<entity::Game, _, _>(&(EntityType::Game, id), accessor)
    }

    pub fn query_team_unique<F, T>(&self, id: Uuid, accessor: F) -> T
        where F: Fn(&entity::Team) -> T, T: Debug + Eq {
        self.query_entity_unique::<entity::Team, _, _>(&(EntityType::Team, id), accessor)
    }

    pub fn debug_subtree(&self, leaf_key: &(EntityType, Uuid)) -> DebugSubtree {
        let mut generations = Vec::new();
        let mut edges = HashMap::new();
        let mut data = HashMap::new();

        let mut next_generation: HashSet<_> = self.roots.get(leaf_key)
            .expect("debug_subtree supplied an invalid entity descriptor")
            .into_iter()
            .cloned()
            .collect();

        while !next_generation.is_empty() {
            let mut new_next_generation = HashSet::new();
            for &idx in &next_generation {
                let (entity, event) = self.graph.node_weight(idx).unwrap();
                data.insert(idx, DebugSubtreeNode {
                    is_ambiguous: entity.is_ambiguous(),
                    is_observed: false,
                    json: json!({
                        "name": entity.description(),
                        "object": entity.to_json(),
                    }),
                });
                let mut child_walker = self.graph.children(idx);
                while let Some((edge_idx, child_idx)) = child_walker.walk_next(&self.graph) {
                    edges.entry(idx).or_insert(Vec::new()).push(child_idx);
                    new_next_generation.insert(child_idx);
                }
            }
            generations.push(next_generation);
            next_generation = new_next_generation;
        }

        DebugSubtree {
            generations,
            edges,
            data,
        }
    }
}