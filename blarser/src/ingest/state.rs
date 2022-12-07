use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::error;
use petgraph::stable_graph::{StableGraph, NodeIndex};
use uuid::Uuid;
use partial_information::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity};
use crate::events::{AffectedEntity, AnyEvent, EarlseasonStart, Event};
use crate::ingest::error::{IngestError, IngestResult};
use crate::ingest::Observation;
use crate::state::EntityType;


#[derive(Default)]
pub struct StateGraph {
    graph: StableGraph<AnyEntity, Arc<AnyEvent>>,
    leafs: HashMap<(EntityType, Uuid), Vec<NodeIndex>>,
    ids_for_type: HashMap<EntityType, Vec<Uuid>>,
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
            .expect("Sim was not found in graph")
            .as_sim()
            .expect("Sim object was not Sim type");

        if sim.phase == 1 && sim.earlseason_date > after {
            vec![AnyEvent::from(EarlseasonStart::new(sim.earlseason_date))]
        } else {
            vec![]
        }
    }

    pub fn ids_for(&self, affected_entity: AffectedEntity) -> Vec<Uuid> {
        if let Some(id) = affected_entity.id() {
            vec![id]
        } else if let Some(d) = self.ids_for_type.get(&affected_entity.ty()) {
            d.clone()
        } else {
            Vec::new()
        }
    }

    pub fn apply_event(&mut self, event: Arc<AnyEvent>, ty: EntityType, id: Uuid) -> IngestResult<Vec<AnyEvent>> {
        let entity_indices = self.leafs.get(&(ty, id))
            .ok_or_else(|| IngestError::EntityDoesNotExist { ty, id })?
            .clone();

        let new_leafs = entity_indices.into_iter()
            .map(|entity_idx| {
                self.apply_event_to_entity(event.clone(), entity_idx)
            })
            .collect();

        let old_leafs = self.leafs.insert((ty, id), new_leafs);
        assert!(old_leafs.is_some(),
                "This insert call should only ever replace existing leafs");

        Ok(Vec::new()) // TODO
    }

    fn apply_event_to_entity(&mut self, event: Arc<AnyEvent>, entity_idx: NodeIndex) -> NodeIndex {
        let entity = self.graph.node_weight(entity_idx)
            .expect("Indices in State.leafs should always be valid");

        let new_entity = match event.as_ref() {
            AnyEvent::Start(e) => { e.forward(entity) }
            AnyEvent::EarlseasonStart(e) => { e.forward(entity) }
            AnyEvent::LetsGo(e) => { e.forward(entity) }
        };

        let new_entity_idx = self.graph.add_node(new_entity);
        self.graph.add_edge(entity_idx, new_entity_idx, event);

        new_entity_idx
    }
}