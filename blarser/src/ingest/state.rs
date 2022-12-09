use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::{error, info};
use daggy::stable_dag::{StableDag, NodeIndex, EdgeIndex};
use petgraph::visit::Walker;
use diesel::PgJsonbExpressionMethods;
use uuid::Uuid;
use partial_information::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity};
use crate::events::{Effect, AnyEvent, EarlseasonStart, Event, AnyExtrapolated, Start};
use crate::ingest::error::{IngestError, IngestResult};
use crate::ingest::Observation;
use crate::state::EntityType;

#[derive(Default)]
pub struct StateGraph {
    graph: StableDag<(AnyEntity, Arc<AnyEvent>), AnyExtrapolated>,
    leafs: HashMap<(EntityType, Uuid), Vec<NodeIndex>>,
    roots: HashSet<NodeIndex>,
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

    pub fn populate(&mut self, obses: Vec<Observation>, start_time: DateTime<Utc>) {
        let start_event: Arc<AnyEvent> = Arc::new(Start::new(start_time).into());
        for obs in obses {
            let entity = AnyEntity::from_raw_json(obs.entity_type, obs.entity_json.clone()) // remove the clone after finished debugging
                .map_err(|e| {
                    error!("{e} for {} {}: {}", obs.entity_type, obs.entity_id, obs.entity_json);
                    e
                })
                .expect("JSON parsing failed");
            let idx = self.graph.add_node((entity, start_event.clone()));
            self.leafs.insert((obs.entity_type, obs.entity_id), vec![idx]);
            self.roots.insert(idx);
            self.ids_for_type.entry(obs.entity_type).or_default().push(obs.entity_id);
        }
    }

    pub fn get_timed_events(&self, after: DateTime<Utc>) -> Vec<AnyEvent> {
        // This function is not intended to be generic. I need to see the natural usage pattern in
        // the normal case before deciding what the general API will look like.
        let sim_idx = self.leafs.get(&(EntityType::Sim, Uuid::nil()))
            .expect("Error: Missing sim leaf")
            .into_iter().exactly_one()
            .expect("There must be exactly one sim node when calling get_timed_events");
        let sim = self.graph.node_weight(*sim_idx)
            .expect("Sim was not found in graph").0
            .as_sim()
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
        let entity_indices = self.leafs.get(&(ty, id))
            .ok_or_else(|| IngestError::EntityDoesNotExist { ty, id })?
            .clone();

        info!("Applying {event} to {ty} {id} with {extrapolated:?}");
        let new_leafs = entity_indices.into_iter()
            .map(|entity_idx| {
                self.apply_event_to_entity(event.clone(), entity_idx, extrapolated)
            })
            .collect();

        let old_leafs = self.leafs.insert((ty, id), new_leafs);
        assert!(old_leafs.is_some(),
                "This insert call should only ever replace existing leafs");

        Ok(Vec::new()) // TODO
    }

    fn apply_event_to_entity(&mut self, event: Arc<AnyEvent>, entity_idx: NodeIndex, extrapolated: &AnyExtrapolated) -> NodeIndex {
        let (entity, _) = self.graph.node_weight(entity_idx)
            .expect("Indices in State.leafs should always be valid");

        dbg!(&extrapolated);

        let new_entity = match event.as_ref() {
            AnyEvent::Start(e) => { e.forward(entity, extrapolated) }
            AnyEvent::EarlseasonStart(e) => { e.forward(entity, extrapolated) }
            AnyEvent::LetsGo(e) => { e.forward(entity, extrapolated) }
            AnyEvent::PlayBall(e) => { e.forward(entity, extrapolated) }
            AnyEvent::HalfInning(e) => { e.forward(entity, extrapolated) }
            AnyEvent::StormWarning(e) => { e.forward(entity, extrapolated) }
            AnyEvent::BatterUp(e) => { e.forward(entity, extrapolated) }
        };

        let new_entity_idx = self.graph.add_node((new_entity, event));
        self.graph.add_edge(entity_idx, new_entity_idx, extrapolated.clone()).unwrap();

        new_entity_idx
    }

    pub fn get_versions_between(&self, entity_type: EntityType, id: Uuid, earliest: DateTime<Utc>, latest: DateTime<Utc>) -> Option<HashSet<NodeIndex>> {
        let Some(leafs) = self.leafs.get(&(entity_type, id)) else {
            return None;
        };

        // I couldn't figure out how to do what I wanted with the built-in graph traversal helpers
        // so I made my own traversal
        let mut stack = leafs.clone();
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

        Some(outputs)
    }

    pub fn version(&self, version_idx: NodeIndex) -> Option<&(AnyEntity, Arc<AnyEvent>)> {
        self.graph.node_weight(version_idx)
    }
}