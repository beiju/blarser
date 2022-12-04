use std::collections::HashMap;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::error;
use petgraph::stable_graph::{StableGraph, NodeIndex};
use uuid::Uuid;
use partial_information::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity};
use crate::events::{AnyEvent, EarlseasonStart};
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
            let entity = AnyEntity::from_raw_json(obs.entity_type, obs.entity_json.clone()) // remove the clone after finished debugging
                .map_err(|e| {
                    error!("{e} for {} {}: {}", obs.entity_type, obs.entity_id, obs.entity_json);
                    e
                })
                .expect("JSON parsing failed");
            let idx = self.graph.add_node(entity);
            self.leafs.insert((obs.entity_type, obs.entity_id), vec![idx]);
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
            .expect("Sim was not found in graph")
            .as_sim()
            .expect("Sim object was not Sim type");

        if sim.phase == 1 && sim.earlseason_date > after {
            vec![AnyEvent::from(EarlseasonStart::new(sim.earlseason_date))]
        } else {
            vec![]
        }
    }
}