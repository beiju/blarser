use std::sync::Arc;
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, map::Map as JsonMap};
use rocket::async_trait;
use rocket::futures::stream::{self, StreamExt};
use async_recursion::async_recursion;

use crate::api::{chronicler, ChroniclerItem};
use crate::blaseball_state as bs;
use crate::ingest::{IngestItem, BoxedIngestItem};
use crate::ingest::error::IngestError;
use crate::ingest::log::IngestLogger;

pub struct ChronUpdate {
    endpoint: &'static str,
    item: ChroniclerItem,
}

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=BoxedIngestItem> + Send>> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |endpoint|
            Box::new(chronicler::versions(endpoint, start)
                .map(|item| Box::new(ChronUpdate { endpoint, item }) as BoxedIngestItem))
                as Box<dyn Iterator<Item=BoxedIngestItem> + Send>
        )
        .collect()
}

#[async_trait]
impl IngestItem for ChronUpdate {
    fn date(&self) -> DateTime<Utc> {
        self.item.valid_from
    }

    async fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> Result<Vec<Arc<bs::BlaseballState>>, IngestError> {
        log.info(format!("Applying chron update from {}", self.item.valid_from)).await?;

        let observation = bs::Observation {
            entity_type: self.endpoint,
            entity_id: self.item.entity_id,
            observed_at: self.item.valid_from,
        };
        let event = bs::Event::ImplicitChange(observation.clone());

        let entity_set = state.data.get(observation.entity_type)
            .expect("Unexpected entity type");
        let mismatches = observe_state(entity_set, &self.item.data, observation).await;

        // If no mismatches, all is well. Return the existing state object, as (conceptually) no
        // changes needed to be made. Filling in placeholders mutates in place and is not considered
        // a change for this purpose.
        if mismatches.is_empty() {
            return Ok(vec![state]);
        }

        let approval_msg = stream::iter(&mismatches)
            .then(|mismatch| mismatch.description(&state))
            .collect::<Vec<Result<_, _>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");

        let approval_msg= format!("From {}/{} at {}: \n{}",
                                  self.endpoint,
                                  self.item.entity_id,
                                  self.item.valid_from,
                                  approval_msg);

        // Otherwise, get approval
        let approved = log.get_approval(
            self.endpoint,
            self.item.entity_id,
            self.item.valid_from,
            approval_msg.clone(),
        ).await?;

        if !approved {
            Err(IngestError::UnexpectedObservation(approval_msg))
        } else {
            Ok(vec![state.successor(event, mismatches)?])
        }
    }
}

async fn observe_state(data: &bs::EntitySet, observed: &JsonValue, observation: bs::Observation) -> Vec<bs::Patch> {
    match data.get(&observation.entity_id) {
        None => vec![
            bs::Patch {
                path: bs::json_path!(observation.entity_type, observation.entity_id),
                change: bs::ChangeType::Add(bs::Node::new_from_json(
                    observed,
                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                    Some(observation),
                )),
            }
        ],
        Some(node) => {
            let path = bs::Path {
                entity_type: observation.entity_type,
                entity_id: Some(observation.entity_id.clone()),
                components: Vec::new(),
            };

            observe_node(node, observed, &observation, &path).await
        }
    }
}

#[async_recursion]
async fn observe_node(node: &bs::Node, observed: &JsonValue, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    match observed {
        JsonValue::Object(map) => {
            observe_object(node, map, observation, path).await
        }
        JsonValue::Array(vec) => {
            observe_array(node, vec, observation, path).await
        }
        JsonValue::String(s) => {
            observe_string(node, s, observation, path).await
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                observe_int(node, i, observation, path)
            } else {
                let f = n.as_f64()
                    .expect("Number could not be interpreted as int or float");
                observe_float(node, f, observation, path)
            }
        }
        JsonValue::Bool(b) => {
            observe_bool(node, b, observation, path)
        }
        JsonValue::Null => {
            observe_null(node, observation, path)
        }
    }
}

async fn observe_object(node: &bs::Node, observed: &JsonMap<String, JsonValue>, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    if let bs::Node::Object(obj) = node {
        let deletions = obj.keys()
            .filter_map(|key| {
                match observed.get(key) {
                    // Value is in state, but not observation: Remove it
                    None => {
                        Some(bs::Patch {
                            path: path.extend(key.into()),
                            change: bs::ChangeType::Remove,
                        })
                    }
                    // Value is in both: it will be processed in the other loop
                    Some(_) => None
                }
            });

        let changes_and_additions = stream::iter(observed.into_iter())
            .then(|(key, value): (&String, &JsonValue)| async move {
                let path = path.extend(key.into());
                match obj.get(key) {
                    // Value is in both objects: Observe it
                    Some(node) => {
                        observe_node(node, value, observation, &path).await
                    }
                    // Value is in observation, but not state: Add it
                    None => {
                        vec![
                            bs::Patch {
                                path,
                                change: bs::ChangeType::Add(bs::Node::new_from_json(
                                    value,
                                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                                    Some(observation.clone()),
                                )),
                            }
                        ]
                    }
                }
            })
            .map(|v| stream::iter(v))
            .flatten();

        stream::iter(deletions)
            .chain(changes_and_additions)
            .collect()
            .await
    } else {
        let caused_by = Arc::new(bs::Event::ImplicitChange(observation.clone()));
        let observation = Some(observation.clone());
        vec![
            bs::Patch {
                path: path.clone(),
                change: bs::ChangeType::Replace(bs::Node::new_from_json_object(observed, &caused_by, &observation)),
            }
        ]
    }
}

async fn observe_array(node: &bs::Node, observed: &Vec<JsonValue>, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    if let bs::Node::Array(arr) = node {
        observe_array_subset(arr, 0, &observed, 0, observation, path).await
    } else {
        let caused_by = Arc::new(bs::Event::ImplicitChange(observation.clone()));
        let observation = Some(observation.clone());
        vec![
            bs::Patch {
                path: path.clone(),
                change: bs::ChangeType::Replace(bs::Node::new_from_json_array(observed, &caused_by, &observation)),
            }
        ]
    }
}

#[async_recursion]
async fn observe_array_subset(
    current_values: &im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &Vec<JsonValue>,
    observed_value_i: usize,
    observation: &bs::Observation,
    path: &bs::Path)
    -> Vec<bs::Patch> {
    match (current_values.get(current_value_i), observed_values.get(observed_value_i)) {
        (Some(current_value), Some(observed_value)) => {
            let item_path = path.extend(current_value_i.into());
            // This clone() is a sign of bad code organization but whatever
            let mut replace_changes = observe_node(current_value, observed_value, observation, &item_path).await;
            let item_matches = replace_changes.is_empty();

            replace_changes.extend(
                observe_array_subset(
                    current_values,
                    current_value_i + 1,
                    observed_values,
                    observed_value_i + 1,
                    observation,
                    path)
                    .await
                    .into_iter()
            );

            if item_matches {
                replace_changes
            } else {
                // Get changes that result from deleting the
                let delete_changes = observe_array_slice_by_deletion(current_values, current_value_i, observed_values, observed_value_i, &observation, &path).await;
                let add_changes = observe_array_slice_by_addition(current_values, current_value_i, observed_values, observed_value_i, &observation, &path, observed_value).await;

                vec![
                    replace_changes,
                    delete_changes,
                    add_changes,
                ].into_iter()
                    .min_by(|a, b| a.len().cmp(&b.len()))
                    .unwrap_or(Vec::new())
            }
        }
        (Some(_), None) => {
            // Item exists in current but not observed. Delete it
            observe_array_slice_by_deletion(current_values, current_value_i, observed_values, observed_value_i, &observation, &path).await
        }
        (None, Some(observed_value)) => {
            // Item exists in observed but not current. Delete it
            observe_array_slice_by_addition(current_values, current_value_i, observed_values, observed_value_i, &observation, &path, observed_value).await
        }
        (None, None) => {
            // Recursion base case. No changes.
            Vec::new()
        }
    }
}

async fn observe_array_slice_by_addition(
    current_values: &im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &Vec<JsonValue>,
    observed_value_i: usize,
    observation: &bs::Observation,
    path: &bs::Path,
    observed_value: &JsonValue) -> Vec<bs::Patch> {
    // Operate on the rest of the vector before this one, because the operation changes indices
    let mut add_changes = observe_array_subset(
        current_values,
        current_value_i,
        observed_values,
        observed_value_i + 1,
        observation,
        path).await;

    add_changes.push(bs::Patch {
        path: path.extend(current_value_i.into()),
        change: bs::ChangeType::Add(bs::Node::new_from_json(
            observed_value,
            Arc::new(bs::Event::ImplicitChange(observation.clone())),
            Some(observation.clone()),
        )),
    });

    add_changes
}

async fn observe_array_slice_by_deletion(
    current_values: &im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &Vec<JsonValue>,
    observed_value_i: usize,
    observation: &bs::Observation,
    path: &bs::Path) -> Vec<bs::Patch> {
    // Operate on the rest of the vector before this one, because the operation changes indices
    let mut delete_changes = observe_array_subset(
        current_values,
        current_value_i + 1,
        observed_values,
        observed_value_i,
        observation,
        path).await;

    delete_changes.push(bs::Patch {
        path: path.extend(current_value_i.into()),
        change: bs::ChangeType::Remove,
    });

    delete_changes
}

async fn observe_string(node: &bs::Node, observed: &String, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    if let bs::Node::Primitive(primitive) = node {
        {
            let primitive = primitive.read().await;
            if let bs::PrimitiveValue::String(value) = &primitive.value {
                if value == observed {
                    return Vec::new();
                }
            }
        }

        vec![
            bs::Patch {
                path: path.clone(),
                change: bs::ChangeType::Replace(bs::Node::successor(
                    primitive.clone(),
                    bs::PrimitiveValue::String(observed.clone()),
                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                    Some(observation.clone()),
                ))
            }
        ]
    } else {
        vec![
            bs::Patch {
                path: path.clone(),
                change: bs::ChangeType::Replace(bs::Node::new_primitive(
                    bs::PrimitiveValue::String(observed.clone()),
                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                    Some(observation.clone()),
                )),
            }
        ]

    }
}

fn observe_int(node: &bs::Node, observed: i64, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    todo!()
}

fn observe_float(node: &bs::Node, observed: f64, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    todo!()
}

fn observe_bool(node: &bs::Node, observed: &bool, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    todo!()
}

fn observe_null(node: &bs::Node, observation: &bs::Observation, path: &bs::Path) -> Vec<bs::Patch> {
    todo!()
}