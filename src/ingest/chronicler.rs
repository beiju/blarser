use std::iter;
use std::sync::Arc;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use itertools::{Itertools, EitherOrBoth};
use serde_json::{Value as JsonValue, map::Map as JsonMap, Value, Map};
use rocket::async_trait;

use crate::api::{chronicler, ChroniclerItem};
use crate::blaseball_state as bs;
use crate::blaseball_state::{Node, Observation, Patch, Path, PrimitiveValue};
use crate::ingest::{IngestItem, BoxedIngestItem, IngestApplyResult};
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

    fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestApplyResult {
        log.info(format!("Applying chron update from {}", self.item.valid_from))?;

        let observation = bs::Observation {
            entity_type: self.endpoint,
            entity_id: self.item.entity_id,
            observed_at: self.item.valid_from,
        };

        let entity_set = state.data.get(observation.entity_type)
            .expect("Unexpected entity type");
        let mismatches: Vec<_> =
            observe_state(log, entity_set, &self.item.data, &observation)
                .try_collect()?;

        // If no mismatches, all is well. Return the existing state object, as (conceptually) no
        // changes needed to be made. Filling in placeholders mutates in place and is not considered
        // a change for this purpose.
        if mismatches.is_empty() {
            return Ok((state, Vec::new()));
        }

        let approval_msg = mismatches.iter()
            .map(|mismatch| mismatch.description(&state))
            .collect::<Vec<Result<_, _>>>()
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
        )?;

        if !approved {
            Err(anyhow!("Unexpected observation: {}", approval_msg))
        } else {
            let event = Arc::new(bs::Event::ImplicitChange(observation));
            let new_state = state.diff_successor(event, mismatches)?;
            Ok((new_state, Vec::new()))
        }
    }
}

type BoxedPatchIterator<'a> = Box<dyn Iterator<Item=Result<bs::Patch, diesel::result::Error>> + 'a>;

fn observe_state<'a>(log: &'a IngestLogger, data: &'a bs::EntitySet, observed: &'a JsonValue, observation: &'a bs::Observation) -> BoxedPatchIterator<'a> {
    match data.get(&observation.entity_id) {
        None => {
            let path = bs::json_path!(observation.entity_type, observation.entity_id);
            emit_new(log, path, observed)
        }
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

fn observe_node<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: &'a JsonValue, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchIterator<'a> {
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

fn observe_object<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: &'a JsonMap<String, JsonValue>, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchIterator<'a> {
    if let bs::Node::Object(obj) = node {
        // Short-circuit for empty source

        let deletions_path = path.clone();
        let deletions = obj.into_iter()
            .filter_map(move |(key, node)| {
                let path = &deletions_path;
                match observed.get(key) {
                    // Value is in state, but not observation: Remove it
                    None => {
                        let log_result = log.info(format!("Observed unexpected removal of value {} at {}", node.to_string(), path))
                            .map(|()| bs::Patch {
                                path: path.extend(key.into()),
                                change: bs::ChangeType::Remove,
                            });
                        Some(log_result)
                    }
                    // Value is in both: it will be processed in the other loop
                    Some(_) => None
                }
            });

        let changes_and_additions = observed.iter()
            .map(move |(key, value)| {
                let path = path.extend(key.into());
                match obj.get(key) {
                    // Value is in both objects: Observe it
                    Some(node) => {
                        observe_node(log, node, value, observation, path)
                    }
                    // Value is in observation, but not state: Add it
                    None => {
                        emit_new(log, path, value)
                    }
                }
            })
            .flatten();

        Box::new(deletions.chain(changes_and_additions))
    } else {
        emit_overwrite(log, node, observed, path)
    }
}

fn emit_overwrite<'a>(log: &'a IngestLogger, node: &'a Node, observed: &'a Map<String, Value>, path: Path) -> BoxedPatchIterator<'a> {
    Box::new(iter::once({
        log.info(format!("Observed an unexpected change at {}: value changed from {} to Object({:?})", path, node.to_string(), observed))
            .map(|()| bs::Patch {
                path,
                change: bs::ChangeType::Overwrite(JsonValue::Object(observed.clone())),
            })
    }))
}

fn emit_new<'a>(log: &'a IngestLogger, path: Path, value: &'a Value) -> BoxedPatchIterator<'a> {
    Box::new(iter::once({
        log.info(format!("Observed an unexpected value at {}", path))
            .map(|()| {
                bs::Patch {
                    path,
                    change: bs::ChangeType::New(value.clone()),
                }
            })
    }))
}

fn observe_array<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: &'a Vec<JsonValue>, observation: &'a bs::Observation, path: bs::Path) -> BoxedPatchIterator<'a> {
    if let bs::Node::Array(arr) = node {
        let it = arr.iter()
            .zip_longest(observed.iter())
            .enumerate()
            .map(move |(i, pair)| {
                let path = path.extend(i.into());
                match pair {
                    EitherOrBoth::Both(node, observed) => {
                        observe_node(log, node, observed, observation, path)
                    }
                    EitherOrBoth::Left(node) => {
                        Box::new(iter::once({
                            log.info(format!("Observed removal at {}: value {}", path, node.to_string()))
                                .map(|()| bs::Patch {
                                    path: path.clone(),
                                    change: bs::ChangeType::Remove,
                                })
                        }))
                    }
                    EitherOrBoth::Right(observed) => {
                        Box::new(iter::once({
                            log.info(format!("Observed new element at {}: value {}", path, observed))
                                .map(|()| bs::Patch {
                                    path: path.clone(),
                                    change: bs::ChangeType::New(observed.clone()),
                                })
                        }))
                    }
                }
            })
            .flatten();

        Box::new(it)
    } else {
        Box::new(iter::once({
            let new_value = JsonValue::Array(observed.clone());
            log.info(format!("Observed an unexpected change at {}: value changed from {} to {}", path, node.to_string(), new_value))
                .map(|()| bs::Patch {
                    path: path.clone(),
                    change: bs::ChangeType::Overwrite(new_value),
                })
        }))
    }
}

fn observe_primitive<'a>(log: &'a IngestLogger, node: &'a bs::Node, observed: PrimitiveValue, observation: &'a bs::Observation, path: bs::Path)
                         -> BoxedPatchIterator<'a> {
    let s = iter::once({
        // This match statement is just to flip Result<Option<T>> to Option<Result<T>>, where an Err
        // result is mapped to Some(Err)
        match observe_primitive_internal(log, node, &observed, observation, path) {
            Ok(None) => { None }
            Ok(Some(patch)) => { Some(Ok(patch)) }
            Err(e) => { Some(Err(e)) }
        }
    })
        .filter_map(|v| v);

    Box::new(s)
}

fn observe_primitive_internal(log: &IngestLogger<'_>, node: &Node, observed: &PrimitiveValue, observation: &Observation, path: Path) -> Result<Option<Patch>, diesel::result::Error> {
    if let bs::Node::Primitive(primitive_node) = node {
        let mut primitive = primitive_node.write().unwrap();
        if match_observation(log, &mut primitive, &observed)? {
            if let None = primitive.observed_by {
                primitive.observed_by = Some(observation.clone());
                // log.info(format!("Observed expected value at {}", path)).await?
            }
            Ok(None)
        } else {
            log.info(format!("Observed changed value at {} from {} to {}", path, primitive.value, observed))?;
            Ok(Some(bs::Patch {
                path,
                change: bs::ChangeType::Set(observed.clone()),
            }))
        }
    } else {
        log.info(format!("Observed changed value at {} from {} to {}", path, node.to_string(), observed))?;
        Ok(Some(bs::Patch {
            path,
            change: bs::ChangeType::Set(observed.clone()),
        }))
    }
}

fn match_observation(log: &IngestLogger<'_>, node: &mut bs::PrimitiveNode, value: &bs::PrimitiveValue) -> Result<bool, diesel::result::Error> {
    let result = match &node.value {
        bs::PrimitiveValue::Null => { value.is_null() }
        bs::PrimitiveValue::Bool(b) => { value.as_bool().map(|value_b| *b == value_b).unwrap_or(false) }
        bs::PrimitiveValue::Int(i) => { value.as_int().map(|value_i| *i == value_i).unwrap_or(false) }
        bs::PrimitiveValue::Float(f) => { value.as_float().map(|value_f| *f == value_f).unwrap_or(false) }
        bs::PrimitiveValue::String(s) => { value.as_str().map(|value_s| s == &value_s).unwrap_or(false) }
        bs::PrimitiveValue::IntRange(lower, upper) => {
            match value.as_int() {
                None => { false }
                Some(i) => {
                    if *lower <= i && i <= *upper {
                        log.info(format!("Observed concrete value {} for int range {}-{}", i, lower, upper))?;
                        node.value = bs::PrimitiveValue::Int(i);
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
                    if *lower <= f && f <= *upper {
                        log.info(format!("Observed concrete value {} for float range {}-{}", f, lower, upper))?;
                        node.value = bs::PrimitiveValue::Float(f);
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
