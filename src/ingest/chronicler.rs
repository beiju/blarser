use std::iter;
use std::pin::Pin;
use std::sync::Arc;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use itertools::{Itertools, EitherOrBoth};
use serde_json::{Value as JsonValue, map::Map as JsonMap, Value};
use rocket::async_trait;
use rocket::futures::stream::{self, Stream, StreamExt, TryStreamExt};

use crate::api::{chronicler, ChroniclerItem};
use crate::blaseball_state as bs;
use crate::blaseball_state::{Node, Observation, Patch, Path, PrimitiveValue};
use crate::ingest::{IngestItem, BoxedIngestItem};
use crate::ingest::error::{IngestError, IngestResult};
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
        .chain(iter::once(
            Box::new(chronicler::game_updates(start)
                .map(|item| Box::new(ChronUpdate { endpoint: "game", item }) as BoxedIngestItem))
                as Box<dyn Iterator<Item=BoxedIngestItem> + Send>
        ))
        .collect()
}

#[async_trait]
impl IngestItem for ChronUpdate {
    fn date(&self) -> DateTime<Utc> {
        self.item.valid_from
    }

    async fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> Result<Arc<bs::BlaseballState>, IngestError> {
        log.info(format!("Applying chron update from {}", self.item.valid_from)).await?;

        let observation = bs::Observation {
            entity_type: self.endpoint,
            entity_id: self.item.entity_id,
            observed_at: self.item.valid_from,
        };

        let entity_set = state.data.get(observation.entity_type)
            .expect("Unexpected entity type");
        let mismatches: Vec<_> =
            observe_state(log, entity_set, &self.item.data, &observation)
                .try_collect::<Vec<_>>()
                .await?;

        // If no mismatches, all is well. Return the existing state object, as (conceptually) no
        // changes needed to be made. Filling in placeholders mutates in place and is not considered
        // a change for this purpose.
        if mismatches.is_empty() {
            return Ok(state);
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
            Err(anyhow!("Unexpected observation: {}", approval_msg))
        } else {
            let event = Arc::new(bs::Event::ImplicitChange(observation));
            state.successor(event, mismatches).await
        }
    }
}

type BoxedPatchStream<'a> = Pin<Box<dyn Stream<Item=IngestResult<bs::Patch>> + Send + 'a>>;

fn observe_state<'a>(log: &'a IngestLogger, data: &'a bs::EntitySet, observed: &'a JsonValue, observation: &'a bs::Observation) -> BoxedPatchStream<'a> {
    match data.get(&observation.entity_id) {
        None => Box::pin(stream::once(async {
            let path = bs::json_path!(observation.entity_type, observation.entity_id);
            log.info(format!("Observed an unexpected value at {}", path)).await?;
            Ok(bs::Patch {
                path,
                change: bs::ChangeType::New(observed.clone()),
            })
        })),
        Some(node) => {
            let path = bs::Path {
                entity_type: observation.entity_type,
                entity_id: Some(observation.entity_id.clone()),
                components: Vec::new(),
            };

            observe_node(log, node, observed, &observation, path)
        }
    }
}

fn observe_node<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: &'a JsonValue, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    let primitive = match observed {
        JsonValue::Object(map) => {
            return observe_object(log, node, map, observation, path);
        }
        JsonValue::Array(vec) => {
            return observe_array(log, node, vec, observation, path);
        }
        Value::Null => { PrimitiveValue::Null }
        Value::Bool(b) => { PrimitiveValue::Bool(*b) }
        Value::Number(n) => { PrimitiveValue::from_json_number(n) }
        Value::String(s) => { PrimitiveValue::String(s.clone()) }
    };

    observe_primitive(log, node, primitive, observation, path)
}

fn observe_object<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: &'a JsonMap<String, JsonValue>, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    if let bs::Node::Object(obj) = node {
        // Short-circuit for empty source

        let deletions_path = path.clone();
        let deletions = stream::iter(obj.into_iter())
            .filter_map(move |(key, node)| {
                let path = deletions_path.clone();
                async move {
                    match observed.get(key) {
                        // Value is in state, but not observation: Remove it
                        None => {
                            let log_result = log.info(format!("Observed unexpected removal of value {} at {}", node.to_string().await, path)).await;
                            // There's probably a better way to do this
                            if let Err(e) = log_result {
                                return Some(Err(e.into()));
                            }
                            Some(Ok(bs::Patch {
                                path: path.extend(key.into()),
                                change: bs::ChangeType::Remove,
                            }))
                        }
                        // Value is in both: it will be processed in the other loop
                        Some(_) => None
                    }
                }
            });

        let changes_and_additions = stream::iter(observed)
            .then(move |(key, value)| {
                let path = path.extend(key.into());

                async move {
                    match obj.get(key) {
                        // Value is in both objects: Observe it
                        Some(node) => {
                            observe_node(log, node, value, observation, path)
                        }
                        // Value is in observation, but not state: Add it
                        None => {
                            Box::pin(stream::once(async {
                                log.info(format!("Observed an unexpected value at {}", path)).await?;
                                Ok(bs::Patch {
                                    path,
                                    change: bs::ChangeType::New(value.clone()),
                                })
                            }))
                        }
                    }
                }
            })
            .flatten();

        Box::pin(deletions.chain(changes_and_additions))
    } else {
        Box::pin(stream::once(async move {
            log.info(format!("Observed an unexpected change at {}: value changed from {} to Object({:?})", path, node.to_string().await, observed)).await?;
            Ok(bs::Patch {
                path,
                change: bs::ChangeType::Overwrite(JsonValue::Object(observed.clone())),
            })
        }))
    }
}

fn observe_array<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: &'a Vec<JsonValue>, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchStream<'a> {
    if let bs::Node::Array(arr) = node {
        let s = stream::iter(arr.iter()
            .zip_longest(observed.iter()))
            .enumerate()
            .map(move |(i, pair)| {
                let path = path.extend(i.into());
                match pair {
                    EitherOrBoth::Both(node, observed) => {
                        observe_node(log, node, observed, observation, path)
                    }
                    EitherOrBoth::Left(node) => {
                        let s = stream::once(async move {
                            log.info(format!("Observed removal at {}: value {}", path, node.to_string().await)).await?;
                            Ok(bs::Patch {
                                path: path.clone(),
                                change: bs::ChangeType::Remove,
                            })
                        });

                        Box::pin(s)
                    }
                    EitherOrBoth::Right(observed) => {
                        let s = stream::once(async move {
                            log.info(format!("Observed new element at {}: value {}", path, observed)).await?;
                            Ok(bs::Patch {
                                path: path.clone(),
                                change: bs::ChangeType::New(observed.clone()),
                            })
                        });

                        Box::pin(s)
                    }
                }
            })
            .flatten();

        Box::pin(s)
    } else {
        Box::pin(stream::once(async move {
            let new_value = JsonValue::Array(observed.clone());
            log.info(format!("Observed an unexpected change at {}: value changed from {} to {}", path, node.to_string().await, new_value)).await?;
            Ok(bs::Patch {
                path: path.clone(),
                change: bs::ChangeType::Overwrite(new_value),
            })
        })) as BoxedPatchStream
    }
}

fn observe_primitive<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: PrimitiveValue, observation: &'a bs::Observation, path: bs::Path)
                         -> BoxedPatchStream<'a> {
    let s = stream::once(async move {
        // This match statement is just to flip Result<Option<T>> to Option<Result<T>>, where an Err
        // result is mapped to Some(Err)
        match observe_primitive_internal(log, node, &observed, observation, path).await {
            Ok(None) => { None }
            Ok(Some(patch)) => { Some(Ok(patch)) }
            Err(e) => { Some(Err(e)) }
        }
    })
        .filter_map(|v| async { v });

    Box::pin(s)
}

async fn observe_primitive_internal(log: &IngestLogger, node: &Node, observed: &PrimitiveValue, observation: &Observation, path: Path) -> IngestResult<Option<Patch>> {
    if let bs::Node::Primitive(primitive_node) = node {
        let mut primitive = primitive_node.write().await;
        if match_observation(log, &mut primitive, &observed).await? {
            if let None = primitive.observed_by {
                primitive.observed_by = Some(observation.clone());
                // log.info(format!("Observed expected value at {}", path)).await?
            }
            Ok(None)
        } else {
            log.info(format!("Observed changed value at {} from {} to {}", path, primitive.value, observed)).await?;
            Ok(Some(bs::Patch {
                path,
                change: bs::ChangeType::Set(observed.clone()),
            }))
        }
    } else {
        log.info(format!("Observed changed value at {} from {} to {}", path, node.to_string().await, observed)).await?;
        Ok(Some(bs::Patch {
            path,
            change: bs::ChangeType::Set(observed.clone()),
        }))
    }
}

async fn match_observation(log: &IngestLogger, node: &mut bs::PrimitiveNode, value: &bs::PrimitiveValue) -> IngestResult<bool> {
    let result = match &node.value {
        bs::PrimitiveValue::Null => { value.is_null() }
        bs::PrimitiveValue::Bool(b) => { value.as_bool().map(|value_b| b == value_b).unwrap_or(false) }
        bs::PrimitiveValue::Int(i) => { value.as_int().map(|value_i| i == value_i).unwrap_or(false) }
        bs::PrimitiveValue::Float(f) => { value.as_float().map(|value_f| f == value_f).unwrap_or(false) }
        bs::PrimitiveValue::String(s) => { value.as_str().map(|value_s| s == value_s).unwrap_or(false) }
        bs::PrimitiveValue::IntRange(lower, upper) => {
            match value.as_int() {
                None => { false }
                Some(i) => {
                    if lower <= i && i <= upper {
                        log.info(format!("Observed concrete value {} for int range {}-{}", i, lower, upper)).await?;
                        node.value = bs::PrimitiveValue::Int(*i);
                        true
                    } else {
                        false
                    }
                }
            }

        }
        bs::PrimitiveValue::FloatRange(lower, upper) => {
            match value.as_float() {
                None => { false }
                Some(f) => {
                    if lower <= f && f <= upper {
                        log.info(format!("Observed concrete value {} for float range {}-{}", f, lower, upper)).await?;
                        node.value = bs::PrimitiveValue::Float(*f);
                        true
                    } else {
                        false
                    }
                }
            }
        }
    };

    Ok(result)
}
