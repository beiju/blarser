use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, map::Map as JsonMap};
use rocket::async_trait;
use rocket::futures::stream::{self, Stream, StreamExt};
use rocket::futures::FutureExt;

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

        let entity_set = state.data.get(observation.entity_type)
            .expect("Unexpected entity type");
        let mismatches: Vec<_> =
            observe_state(entity_set, &self.item.data, &observation)
                .collect::<Vec<_>>()
                .await;

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

        let approval_msg = format!("From {}/{} at {}: \n{}",
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
            let event = bs::Event::ImplicitChange(observation);
            Ok(vec![state.successor(event, mismatches)?])
        }
    }
}

type BoxedPatchStream<'a> = Pin<Box<dyn Stream<Item=bs::Patch> + Send + 'a>>;

fn observe_state<'a>(data: &'a bs::EntitySet, observed: &'a JsonValue, observation: &'a bs::Observation) -> BoxedPatchStream<'a> {
    match data.get(&observation.entity_id) {
        None => Box::pin(stream::once(async {
            bs::Patch {
                path: bs::json_path!(observation.entity_type, observation.entity_id),
                change: bs::ChangeType::Add(bs::Node::new_from_json(
                    observed,
                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                    Some(observation.clone()),
                )),
            }
        })),
        Some(node) => {
            let path = bs::Path {
                entity_type: observation.entity_type,
                entity_id: Some(observation.entity_id.clone()),
                components: Vec::new(),
            };

            observe_node(node, observed, &observation, path)
        }
    }
}

fn optional_stream<'a>(val: impl Future<Output=Option<bs::Patch>> + 'a) -> Box<dyn Stream<Item=bs::Patch> + 'a> {
    Box::new(stream::once(val)
        .filter_map(|v| async { v }))
}

fn observe_node<'a>(node: &'a bs::Node, observed: &'a JsonValue, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    match observed {
        JsonValue::Object(map) => {
            observe_object(node, map, observation, path)
        }
        JsonValue::Array(vec) => {
            observe_array(node, vec, observation, path)
        }
        JsonValue::String(s) => {
            observe_string(node, s, observation, path)
        }
        _ => { todo!() }
        /*JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                optional_stream(observe_int(node, i, observation, path))
            } else {
                let f = n.as_f64()
                    .expect("Number could not be interpreted as int or float");
                optional_stream(observe_float(node, f, observation, path))
            }
        }
        JsonValue::Bool(b) => {
            optional_stream(observe_bool(node, b, observation, path))
        }
        JsonValue::Null => {
            optional_stream(observe_null(node, observation, path))
        }*/
    }
}

fn observe_object<'a>(node: &'a bs::Node, observed: &'a JsonMap<String, JsonValue>, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    if let bs::Node::Object(obj) = node {
        let base_path = path.clone();
        let deletions = obj.keys()
            .filter_map(move |key| {
                match observed.get(key) {
                    // Value is in state, but not observation: Remove it
                    None => {
                        Some(bs::Patch {
                            path: base_path.extend(key.into()),
                            change: bs::ChangeType::Remove,
                        })
                    }
                    // Value is in both: it will be processed in the other loop
                    Some(_) => None
                }
            });

        let changes_and_additions = stream::iter(observed)
            .map(move |(key, value)| {
                let path = path.extend(key.into());
                match obj.get(key) {
                    // Value is in both objects: Observe it
                    Some(node) => {
                        observe_node(node, value, observation, path)
                    }
                    // Value is in observation, but not state: Add it
                    None => {
                        Box::pin(stream::once(async {
                            bs::Patch {
                                path,
                                change: bs::ChangeType::Add(bs::Node::new_from_json(
                                    value,
                                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                                    Some(observation.clone()),
                                )),
                            }
                        }))
                    }
                }
            })
            .flatten();

        Box::pin(stream::iter(deletions).chain(changes_and_additions))
    } else {
        let caused_by = Arc::new(bs::Event::ImplicitChange(observation.clone()));
        let observation = Some(observation.clone());
        Box::pin(stream::once(async move {
            bs::Patch {
                path,
                change: bs::ChangeType::Replace(bs::Node::new_from_json_object(observed, &caused_by, &observation)),
            }
        }))
    }
}

fn observe_array<'a>(node: &'a bs::Node, observed: &'a Vec<JsonValue>, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    if let bs::Node::Array(arr) = node {
        observe_array_subset(arr, 0, &observed, 0, observation, path)
    } else {
        let caused_by = Arc::new(bs::Event::ImplicitChange(observation.clone()));
        let observation = Some(observation.clone());
        Box::pin(stream::once(async move {
            bs::Patch {
                path: path.clone(),
                change: bs::ChangeType::Replace(bs::Node::new_from_json_array(observed, &caused_by, &observation)),
            }
        })) as BoxedPatchStream
    }
}

fn observe_array_subset<'a>(
    current_values: &'a im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &'a Vec<JsonValue>,
    observed_value_i: usize,
    observation: &'a bs::Observation,
    path: bs::Path)
    -> BoxedPatchStream<'a> {
    match (current_values.get(current_value_i), observed_values.get(observed_value_i)) {
        (Some(current_value), Some(observed_value)) => {
            let replace_changes = observe_array_slice_by_replacement(current_values, current_value_i, observed_values, observed_value_i, &observation, path.clone(), current_value, observed_value);
            let delete_changes = observe_array_slice_by_deletion(current_values, current_value_i, observed_values, observed_value_i, &observation, path.clone());
            let add_changes = observe_array_slice_by_addition(current_values, current_value_i, observed_values, observed_value_i, &observation, path, observed_value);

            // I'm so, so sorry
            Box::pin(
                stream::once(async {
                    let possible_changes = stream::iter(vec![
                        replace_changes,
                        delete_changes,
                        add_changes,
                    ])
                        .then(|stream| StreamExt::collect::<Vec<_>>(stream))
                        .collect::<Vec<_>>()
                        .await;
                    let best_change = possible_changes.into_iter()
                        .min_by(|a, b| a.len().cmp(&b.len()))
                        .unwrap_or(Vec::new());

                    stream::iter(best_change)
                })
                    .flatten()
            )
        }
        (Some(_), None) => {
            // Item exists in current but not observed. Delete it
            observe_array_slice_by_deletion(current_values, current_value_i, observed_values, observed_value_i, &observation, path)
        }
        (None, Some(observed_value)) => {
            // Item exists in observed but not current. Delete it
            observe_array_slice_by_addition(current_values, current_value_i, observed_values, observed_value_i, &observation, path, observed_value)
        }
        (None, None) => {
            // Recursion base case. No changes.
            Box::pin(stream::empty())
        }
    }
}

fn observe_array_slice_by_replacement<'a>(
    current_values: &'a im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &'a Vec<JsonValue>,
    observed_value_i: usize,
    observation: &'a bs::Observation,
    path: bs::Path,
    current_value: &'a bs::Node,
    observed_value: &'a JsonValue) -> BoxedPatchStream<'a> {
    // Operate on the rest of the vector before this one, because the operation changes indices
    let item_path = path.extend(current_value_i.into());
    let stream = observe_array_subset(
        current_values,
        current_value_i + 1,
        observed_values,
        observed_value_i + 1,
        observation,
        path)
        .chain(observe_node(current_value, observed_value, observation, item_path));

    Box::pin(stream)
}


fn observe_array_slice_by_addition<'a>(
    current_values: &'a im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &'a Vec<JsonValue>,
    observed_value_i: usize,
    observation: &'a bs::Observation,
    path: bs::Path,
    observed_value: &'a JsonValue) -> BoxedPatchStream<'a> {
    // Operate on the rest of the vector before this one, because the operation changes indices
    let stream = observe_array_subset(
        current_values,
        current_value_i,
        observed_values,
        observed_value_i + 1,
        observation,
        path.clone())
        .chain(stream::once(async move {
            bs::Patch {
                path: path.extend(current_value_i.into()),
                change: bs::ChangeType::Add(bs::Node::new_from_json(
                    observed_value,
                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                    Some(observation.clone()),
                )),
            }
        }));

    Box::pin(stream)
}

fn observe_array_slice_by_deletion<'a>(
    current_values: &'a im::Vector<bs::Node>,
    current_value_i: usize,
    observed_values: &'a Vec<JsonValue>,
    observed_value_i: usize,
    observation: &'a bs::Observation,
    path: bs::Path) -> BoxedPatchStream<'a> {
    // Operate on the rest of the vector before this one, because the operation changes indices
    let stream = observe_array_subset(
        current_values,
        current_value_i + 1,
        observed_values,
        observed_value_i,
        observation,
        path.clone())
        .chain(stream::once(async move {
            bs::Patch {
                path: path.extend(current_value_i.into()),
                change: bs::ChangeType::Remove,
            }
        }));

    Box::pin(stream)
}

fn observe_string<'a>(node: &'a bs::Node, observed: &'a String, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    if let bs::Node::Primitive(primitive_node) = node {
        let s = stream::once(async move {
            let primitive = primitive_node.read().await;
            if let bs::PrimitiveValue::String(value) = &primitive.value {
                if value == observed {
                    None
                } else {
                    Some(bs::Patch {
                        path,
                        change: bs::ChangeType::Replace(bs::Node::successor(
                            primitive_node.clone(),
                            bs::PrimitiveValue::String(observed.clone()),
                            Arc::new(bs::Event::ImplicitChange(observation.clone())),
                            Some(observation.clone()),
                        )),
                    })
                }
            } else {
                Some(bs::Patch {
                    path,
                    change: bs::ChangeType::Replace(bs::Node::successor(
                        primitive_node.clone(),
                        bs::PrimitiveValue::String(observed.clone()),
                        Arc::new(bs::Event::ImplicitChange(observation.clone())),
                        Some(observation.clone()),
                    )),
                })
            }
        });

        Box::pin(s.filter_map(|patch| async move { patch }))
    } else {
        let s = stream::once(async {
            bs::Patch {
                path,
                change: bs::ChangeType::Replace(bs::Node::new_primitive(
                    bs::PrimitiveValue::String(observed.clone()),
                    Arc::new(bs::Event::ImplicitChange(observation.clone())),
                    Some(observation.clone()),
                )),
            }
        });

        Box::pin(s)
    }
}

async fn observe_int(node: &bs::Node, observed: i64, observation: &bs::Observation, path: &bs::Path) -> Option<bs::Patch> {
    todo!()
}

async fn observe_float(node: &bs::Node, observed: f64, observation: &bs::Observation, path: &bs::Path) -> Option<bs::Patch> {
    todo!()
}

async fn observe_bool(node: &bs::Node, observed: &bool, observation: &bs::Observation, path: &bs::Path) -> Option<bs::Patch> {
    todo!()
}

async fn observe_null(node: &bs::Node, observation: &bs::Observation, path: &bs::Path) -> Option<bs::Patch> {
    todo!()
}