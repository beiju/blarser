use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::iter;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use daggy::stable_dag::{StableDag, NodeIndex, EdgeIndex};
use petgraph::visit::Walker;
use serde::Serialize;
use uuid::Uuid;

use crate::entity::{self, AnyEntity, Entity};
use crate::events::{Effect, AnyEvent, EarlseasonStart, Event, AnyExtrapolated, Start, with_any_event};
use crate::ingest::error::IngestResult;
use crate::ingest::{GraphDebugHistory, Observation};
use crate::ingest::task::{DebugHistoryItem, DebugHistoryVersion, DebugTree, DebugTreeNode};
use crate::state::EntityType;

#[derive(Debug, Copy, Clone, Serialize)]
pub enum AddedReason {
    Start,
    NewFromEvent,
    RefinedFromObservation,
    DescendantOfObservedNode,
}

#[derive(Debug, Clone)]
pub struct StateGraphNode {
    pub entity: AnyEntity,
    pub event: Arc<AnyEvent>,
    pub observed: Option<Arc<Observation>>,
    // For debugging mostly
    pub added_reason: AddedReason,
}

impl StateGraphNode {
    pub fn new_observed(
        entity: AnyEntity,
        event: Arc<AnyEvent>,
        observation: Arc<Observation>,
        added_reason: AddedReason,
    ) -> Self {
        Self {
            entity,
            event,
            observed: Some(observation),
            added_reason,
        }
    }
}

pub type StateGraphEdge = AnyExtrapolated;

#[derive(Default, Clone)]
pub struct EntityStateGraph {
    pub(crate) graph: StableDag<StateGraphNode, StateGraphEdge>,
    roots: Vec<NodeIndex>,
    leafs: Vec<NodeIndex>,
}

impl EntityStateGraph {
    pub fn new(first_node: StateGraphNode) -> Self {
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

    pub fn add_root(&mut self, idx: NodeIndex) {
        self.roots.push(idx)
    }

    pub fn get_version(&self, idx: NodeIndex) -> Option<&StateGraphNode> {
        self.graph.node_weight(idx)
    }

    pub fn get_version_mut(&mut self, idx: NodeIndex) -> Option<&mut StateGraphNode> {
        self.graph.node_weight_mut(idx)
    }

    pub fn add_child_version(&mut self,
                             parent_idx: NodeIndex,
                             new_entity: AnyEntity,
                             event: Arc<AnyEvent>,
                             extrapolated: AnyExtrapolated,
                             added_reason: AddedReason,
    ) -> NodeIndex {
        let child_idx = self.graph.add_node(StateGraphNode {
            entity: new_entity,
            event,
            observed: None,
            added_reason,
        });
        self.graph.add_edge(parent_idx, child_idx, extrapolated.clone()).unwrap();
        child_idx
    }

    pub fn add_child_disconnected(&mut self,
                                  new_entity: AnyEntity,
                                  event: Arc<AnyEvent>,
                                  added_reason: AddedReason,
    ) -> NodeIndex {
        self.graph.add_node(StateGraphNode {
            entity: new_entity,
            event,
            observed: None,
            added_reason,
        })
    }


    pub fn add_observed_child_disconnected(&mut self,
                                           new_entity: AnyEntity,
                                           event: Arc<AnyEvent>,
                                           added_reason: AddedReason,
                                           obs: Arc<Observation>,
    ) -> NodeIndex {
        self.graph.add_node(StateGraphNode {
            entity: new_entity,
            event,
            observed: Some(obs),
            added_reason,
        })
    }

    pub fn add_edge(&mut self, from: NodeIndex, to: NodeIndex, weight: AnyExtrapolated) -> EdgeIndex {
        self.graph.add_edge(from, to, weight)
            .expect("Adding edge would cycle")
    }

    pub fn remove_edge(&mut self, idx: EdgeIndex) -> Option<StateGraphEdge> {
        self.graph.remove_edge(idx)
    }

    pub fn remove_node(&mut self, idx: NodeIndex) -> Option<StateGraphNode> {
        self.graph.remove_node(idx)
    }

    pub fn get_candidate_placements(&self, earliest: DateTime<Utc>, latest: DateTime<Utc>) -> HashSet<NodeIndex> {
        // I couldn't figure out how to do what I wanted with the built-in graph traversal helpers
        // so I made my own traversal
        let mut stack = self.leafs.clone();
        let mut visited = HashSet::new();
        let mut outputs = HashSet::new();
        while let Some(node_idx) = stack.pop() {
            visited.insert(node_idx);
            // Get time span of this node
            let node = self.graph.node_weight(node_idx)
                .expect("Stack contained a node that was not in the graph");
            let earliest_node_time: DateTime<Utc> = node.event.time();
            let mut latest_node_time = None;
            let mut child_walker = self.graph.children(node_idx);
            while let Some((_, child_idx)) = child_walker.walk_next(&self.graph) {
                let parent_event = &self.graph.node_weight(child_idx)
                    .expect("Graph gave me an invalid index")
                    .event;
                if let Some(prev_time) = latest_node_time.replace(parent_event.time()) {
                    assert_eq!(prev_time, parent_event.time(),
                               "All children of the same node must have the same time");
                }
            }
            // If this node's time span ends before the observation's time span begins, we can stop
            // traversing its branch and not add it to outputs
            if latest_node_time.map_or(false, |t| t < earliest) { continue; }
            // I thought you could stop walking if you hit an already-observed node but, alas, nope
            let mut parent_walker = self.graph.parents(node_idx);
            while let Some((_, parent_idx)) = parent_walker.walk_next(&self.graph) {
                if !visited.contains(&parent_idx) { stack.push(parent_idx); }
            }
            // If this node's time span begins after the observation's time span ends, we continue
            // traversing its branch but don't add it to outputs
            if earliest_node_time > latest { continue; }
            outputs.insert(node_idx);
        }

        outputs
    }

    pub fn apply_event(&mut self, event: Arc<AnyEvent>, extrapolated: &AnyExtrapolated) {
        let new_leafs = self.leafs.clone().into_iter()
            .map(|entity_idx| {
                self.apply_event_to_entity(event.clone(), entity_idx, extrapolated)
            })
            .collect();

        self.leafs = new_leafs;
    }

    fn apply_event_to_entity(&mut self, event: Arc<AnyEvent>, entity_idx: NodeIndex, extrapolated: &AnyExtrapolated) -> NodeIndex {
        let entity = &self.get_version(entity_idx)
            .expect("Indices in State.leafs should always be valid")
            .entity;
        
        let new_entity = with_any_event!(event.as_ref(), |e| { 
            e.forward(entity, extrapolated) 
        });

        self.add_child_version(entity_idx, new_entity, event, extrapolated.clone(), AddedReason::NewFromEvent)
    }

    pub fn get_debug_tree(&self) -> DebugTree {
        let mut generations = Vec::new();
        let mut edges = HashMap::new();
        let mut data = HashMap::new();

        let mut order_map = HashMap::new();
        let mut next_order_num: usize = 0;
        for &root in &self.roots {
            let mut dfs = petgraph::visit::Dfs::new(&self.graph, root);
            while let Some(node) = dfs.next(&self.graph) {
                order_map.insert(node, next_order_num);
                next_order_num += 1;
            }
        }

        let mut next_generation: HashSet<_> = self.roots.iter()
            .cloned()
            .collect();

        while !next_generation.is_empty() {
            let mut new_next_generation = HashSet::new();
            for &idx in &next_generation {
                let node = self.graph.node_weight(idx).unwrap();
                data.insert(idx, DebugTreeNode {
                    description: node.entity.description(),
                    is_ambiguous: node.entity.is_ambiguous(),
                    observed: node.observed.as_ref().map(|obs| obs.perceived_at),
                    added_reason: node.added_reason,
                    json: node.entity.to_json(),
                    order: *order_map.get(&idx)
                        .expect("Every index reachable from a root should be in order_map"),
                });
                let mut child_walker = self.graph.children(idx);
                while let Some((_, child_idx)) = child_walker.walk_next(&self.graph) {
                    edges.entry(idx).or_insert(Vec::new()).push(child_idx);
                    new_next_generation.insert(child_idx);
                }
            }
            generations.push(next_generation);
            next_generation = new_next_generation;
        }

        DebugTree {
            generations,
            edges,
            data,
            roots: self.roots.clone(),
            leafs: self.leafs.clone(),
        }
    }
}

#[derive(Default)]
pub struct StateGraph {
    pub(crate) graphs: HashMap<(EntityType, Uuid), EntityStateGraph>,
    ids_for_type: HashMap<EntityType, Vec<Uuid>>,
}

impl StateGraph {
    pub fn new() -> Self { Default::default() }

    pub fn populate(&mut self, obses: Vec<Observation>, start_time: DateTime<Utc>, history: &mut GraphDebugHistory) {
        let start_event: Arc<AnyEvent> = Arc::new(Start::new(start_time).into());
        for obs in obses {
            let entity = AnyEntity::from_raw(obs.entity_raw.clone());

            // Unfortunately these assignments all have to be in a specific order that makes it
            // not particularly easy to tell what's going on. Gathering data for the debug view is
            // interleaved with meaningful work.
            // Debug
            let entity_human_name = entity.to_string();
            let description = entity.description();
            let json = entity.to_json();
            let time = obs.perceived_at;

            // Real work
            let entity_type = obs.entity_type;
            let entity_id = obs.entity_id;
            let new_graph = EntityStateGraph::new(StateGraphNode::new_observed(
                entity, start_event.clone(), Arc::new(obs), AddedReason::Start));

            // Debug
            let generations = vec![new_graph.roots().iter().cloned().collect()];
            let idx = *new_graph.roots().iter().exactly_one().unwrap();

            // Real work
            self.graphs.insert((entity_type, entity_id), new_graph);
            self.ids_for_type.entry(entity_type).or_default().push(entity_id);

            // Debug
            history.insert((entity_type, entity_id), DebugHistoryItem {
                entity_human_name,
                versions: vec![DebugHistoryVersion {
                    event_human_name: "Start".to_string(),
                    time: start_time,
                    tree: DebugTree {
                        generations,
                        edges: Default::default(),
                        data: iter::once((idx, DebugTreeNode {
                            description,
                            is_ambiguous: false, // can't be ambiguous at start
                            observed: Some(time),
                            added_reason: AddedReason::Start,
                            json,
                            order: 0,
                        })).collect(),
                        roots: vec![idx],
                        leafs: vec![idx],
                    },
                    queued_for_update: None,
                    currently_updating: None,
                    queued_for_delete: None,
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
        let entity = &sim_graph.get_version(*sim_idx)
            .expect("Sim was not found in graph")
            .entity;
        let sim: &entity::Sim = entity.try_into()
            .expect("Sim object was not Sim type");

        if sim.phase == 1 && sim.earlseason_date > after {
            vec![AnyEvent::from(EarlseasonStart::new(sim.earlseason_date, sim.season))]
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

    fn query_entity_unique<EntityT: Entity, F, T>(&self, leaf_id: &(EntityType, Uuid), accessor: F) -> T
        where F: Fn(&EntityT) -> T,
              T: Debug + Eq,
              for<'a> &'a AnyEntity: TryInto<&'a EntityT>,
              for<'a> <&'a AnyEntity as TryInto<&'a EntityT>>::Error: Debug {
        // TODO Don't take these paramters as a tuple any more
        let graph = self.entity_graph(leaf_id.0, leaf_id.1)
            .expect("Entity not found. TODO: Make this a Result type");
        let mut result = None;
        for &leaf in &graph.leafs {
            let entity = &graph.get_version(leaf)
                .expect("Leafs should never have an invalid index")
                .entity;
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
    
    pub fn games_for_day(&self, season: i32, day: i32) -> impl Iterator<Item=Uuid> + '_ {
        self.ids_for_type.get(&EntityType::Game)
            .expect("Game entity type must exist here")
            .iter()
            .filter(move |&&game_id| {
                self.query_game_unique(game_id, |game| {
                    game.season == season && game.day == day
                })
            })
            .cloned()
    }
}