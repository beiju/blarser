use std::collections::HashMap;
use petgraph::stable_graph::{StableGraph, NodeIndex};
use uuid::Uuid;
use partial_information::PartialInformationCompare;


use crate::entity;
use crate::entity::{AnyEntity, Entity};
use crate::events::{AnyEvent, Event};
use crate::ingest::Observation;
use crate::state::EntityType;


#[derive(Default)]
pub struct StateGraph {
    graph: StableGraph<AnyEntity, AnyEvent>,
    leafs: HashMap<(EntityType, Uuid), Vec<NodeIndex>>,
}

fn insert_from_observation<EntityT: Entity + PartialInformationCompare>(vec: &mut Vec<EntityT>, raw_json: serde_json::Value) {
    let raw: EntityT::Raw = serde_json::from_value(raw_json)
        .expect("TODO handle errors");
    let entity = EntityT::from_raw(raw);
    vec.push(entity);
}

impl StateGraph {
    pub fn populate(&mut self, obses: Vec<Observation>) {
        for obs in obses {
            let entity = AnyEntity::from_raw_json(obs.entity_type, obs.entity_json)
                .expect("JSON parsing failed");
            let idx = self.graph.add_node(entity);
            self.leafs.insert((obs.entity_type, obs.entity_id), vec![idx]);
        }
    }
}