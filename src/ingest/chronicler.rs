use std::collections;
use std::collections::HashSet;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue};
use im::hashmap::Entry;
use rocket::async_trait;
use uuid::Uuid;

use crate::api::{chronicler, ChroniclerItem};
use crate::blaseball_state as bs;
use crate::blaseball_state::{Observation, ValueChange};
use crate::ingest::IngestItem;
use crate::ingest::error::IngestError;
use crate::ingest::log::IngestLogger;

pub struct ChronUpdate {
    endpoint: &'static str,
    item: ChroniclerItem,
}

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=Box<dyn IngestItem + Send>> + Send>> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |endpoint|
            Box::new(chronicler::versions(endpoint, start)
                .map(|item| Box::new(ChronUpdate { endpoint, item }) as Box<dyn IngestItem + Send>))
                as Box<dyn Iterator<Item=Box<dyn IngestItem + Send>> + Send>
        )
        .collect()
}

#[async_trait]
impl IngestItem for ChronUpdate {
    fn date(&self) -> DateTime<Utc> {
        self.item.valid_from
    }

    async fn apply(self: Box<Self>, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> Result<Vec<Arc<bs::BlaseballState>>, IngestError> {
        let observation = bs::Observation {
            entity_type: self.endpoint,
            entity_id: self.item.entity_id,
            observed_at: self.item.valid_from,
        };

        log.info(format!("Applying chron update from {}", self.item.valid_from)).await?;
        match apply_update(&state, log, &observation, &self.item.data).await? {
            None => Ok(state),
            Some(diff) => try_implicit_change(log, state.clone(), observation, diff).await
        }
    }
}

pub async fn apply_update<'a>(
    state: &'a Arc<bs::BlaseballState>,
    log: &IngestLogger,
    observation: &bs::Observation,
    data: &'a JsonValue,
) -> Result<Vec<bs::ValueChange>, IngestError> {
    log.debug(format!("Applying Chron {} update", observation.entity_type)).await?;
    let entity_state = &state.data[observation.entity_type][&observation.entity_id];
    Ok(apply_entity_update(entity_state, &data, bs::Path {
        entity_type: observation.entity_type,
        entity_id: observation.entity_id,
        components: vec![],
    }).err().unwrap_or(vec![]))
}

fn apply_entity_update<'a>(entity_state: &'a bs::Value, entity_update: &'a JsonValue, path: bs::Path) -> Result<(), Vec<ValueChange>> {
    match entity_state {
        bs::Value::Object(state_obj) => {
            let update_obj = entity_update.as_object()
                .ok_or(vec![
                    ValueChange::SetValue {
                        path,
                        value: Default::default()
                    }
                ])?;
            let state_keys: HashSet<_> = state_obj.keys().into_iter().collect();
            let update_keys: HashSet<_> = update_obj.keys().into_iter().collect();

            let missing_keys: Vec<String> = (&state_keys - &update_keys).iter().cloned().cloned().collect();
            if !missing_keys.is_empty() {
                return Err(bs::ValueDiff::KeysRemoved(missing_keys));
            }

            let extra_keys: Vec<_> = update_keys.difference(&state_keys).collect();
            if !extra_keys.is_empty() {
                return Err(bs::ValueDiff::KeysAdded(
                    extra_keys.into_iter()
                        .map(|&key| (key.clone(), &entity_update[key]))
                        .collect()
                ));
            }

            let nested_errs: collections::HashMap<_, _> = update_obj.into_iter().filter_map(|(key, value)|
                match apply_entity_update(&state_obj[key], value) {
                    Ok(()) => None,
                    Err(e) => Some((key.to_string(), e))
                })
                .collect();

            if nested_errs.is_empty() {
                Ok(())
            } else {
                Err(bs::ValueDiff::ObjectDiff { 0: nested_errs })
            }
        }
        bs::Value::Array(state_arr) => {
            let update_arr = entity_update.as_array()
                .ok_or(bs::ValueDiff::ValueChanged {
                    before: entity_state,
                    after: entity_update,
                })?;

            if state_arr.len() != update_arr.len() {
                return Err(bs::ValueDiff::ArraySizeChanged {
                    before: state_arr.len(),
                    after: update_arr.len(),
                });
            }

            let nested_errs: collections::HashMap<_, _> = itertools::enumerate(itertools::zip(state_arr, update_arr))
                .filter_map(|(i, (state_item, update_item))|
                    match apply_entity_update(state_item, update_item) {
                        Ok(_) => None,
                        Err(e) => Some((i, e))
                    })
                .collect();

            if nested_errs.is_empty() {
                Ok(())
            } else {
                Err(bs::ValueDiff::ArrayDiff { 0: nested_errs })
            }
        }
        bs::Value::Value(state_val) => {
            match &state_val.value {
                bs::PropertyValue::Null => {
                    entity_update.as_null()
                        .ok_or(bs::ValueDiff::ValueChanged {
                            before: entity_state,
                            after: entity_update,
                        })
                }
                bs::PropertyValue::Bool(state_bool) => {
                    entity_update.as_bool()
                        .filter(|v| state_bool == v)
                        .ok_or(bs::ValueDiff::ValueChanged {
                            before: entity_state,
                            after: entity_update,
                        })
                        .map(|_| ())
                }
                bs::PropertyValue::Int(state_int) => {
                    entity_update.as_i64()
                        .filter(|v| state_int == v)
                        .ok_or(bs::ValueDiff::ValueChanged {
                            before: entity_state,
                            after: entity_update,
                        })
                        .map(|_| ())
                }
                bs::PropertyValue::Double(state_double) => {
                    entity_update.as_f64()
                        .filter(|v| state_double == v)
                        .ok_or(bs::ValueDiff::ValueChanged {
                            before: entity_state,
                            after: entity_update,
                        })
                        .map(|_| ())
                }
                bs::PropertyValue::String(state_str) => {
                    entity_update.as_str()
                        .filter(|v| state_str == v)
                        .ok_or(bs::ValueDiff::ValueChanged {
                            before: entity_state,
                            after: entity_update,
                        })
                        .map(|_| ())
                }
                _ => todo!()
            }
        }
    }
}

fn try_implicit_change<'a>(
    log: &'a IngestLogger,
    state: Arc<bs::BlaseballState>,
    observation: bs::Observation,
    diff: bs::ValueDiff<'a>,
) -> impl std::future::Future<Output=Result<Arc<bs::BlaseballState>, IngestError>> + 'a {
    async move {
        log.debug("Update didn't match current state; trying as implicit change".to_string()).await?;
        if log.get_approval(observation.entity_type, observation.entity_id, observation.observed_at, diff.format()).await? {
            log.debug("Applying update as implicit change".to_string()).await?;
            return Ok(implicit_change(state, observation, diff));
        }

        Err(IngestError::UpdateMismatch { observation, diff: diff.format() })
    }
}

fn implicit_change(state: Arc<bs::BlaseballState>, observation: Observation, diff: bs::ValueDiff) -> Arc<bs::BlaseballState> {
    let new_data = apply_diff(&state, observation.entity_type, &observation.entity_id, diff);
    Arc::new(bs::BlaseballState {
        predecessor: Some(state),
        from_event: Arc::new(bs::Event::new_implicit_change(observation)),
        data: new_data,
    })
}

fn apply_diff(state: &bs::BlaseballState, endpoint: &'static str, entity_id: &Uuid, diff: bs::ValueDiff) -> im::HashMap<&'static str, bs::EntitySet> {
    state.data.alter(|entity_set|
                         match entity_set {
                             Some(entity_set) => Some(entity_set.alter(|entity| match entity {
                                 Some(data) => Some(apply_diff_to_value(&data, diff)),
                                 None => panic!("Tried to apply structural update to nonexistent object")
                             }, *entity_id)),
                             None => panic!("Tried to apply structural update to nonexistent endpoint")
                         }, endpoint)
}

fn new_primitive_value(value: bs::PropertyValue, caused_by: Arc<bs::Event>) -> bs::Value {
    // If this was caused_by an implicit change, which has an observation attached, automatically
    // fill in observed_by.
    let observed_by = match &*caused_by {
        bs::Event::ImplicitChange(observation) => Some(observation.clone()),
        _ => None
    };

    bs::Value::Value(Arc::new(bs::TrackedValue {
        predecessor: None,
        caused_by,
        observed_by,
        value,
    }))
}

fn apply_diff_to_value(value: &bs::Value, diff: bs::ValueDiff, caused_by: Arc<bs::Event>) -> bs::Value {
    match diff {
        bs::ValueDiff::KeysRemoved(keys) => {
            if let bs::Value::Object(obj) = value {
                let mut obj = obj.clone();
                for key in keys {
                    obj.remove(&*key);
                }
                bs::Value::Object(obj)
            } else {
                panic!("Can't apply a KeysRemoved diff to a non-object");
            }
        }
        bs::ValueDiff::KeysAdded(children) => {
            if let bs::Value::Object(obj) = value {
                let mut obj = obj.clone();
                for (key, value) in children {
                    obj.insert(key, json_to_state_value(value, caused_by));
                }

                bs::Value::Object(obj)
            } else {
                panic!("Can't apply a KeysRemoved diff to a non-object");
            }
        }
        bs::ValueDiff::ArraySizeChanged { after, .. } => {
            if let bs::Value::Array(_) = value {
                if after == 0 {
                    bs::Value::Array(im::Vector::new())
                } else {
                    panic!("Can't apply a non-0 ArraySizeChanged diff as a structural update")
                }
            } else {
                panic!("Can't apply an ArraySizeChanged diff to a non-array")
            }
        }
        bs::ValueDiff::ValueChanged { after, .. } => json_to_state_value(after),
        bs::ValueDiff::ObjectDiff(changes) => {
            if let bs::Value::Object(obj) = value {
                let mut obj = obj.clone();
                for (key, diff) in changes {
                    match obj.entry(key.clone()) {
                        Entry::Occupied(mut entry) => {
                            entry.insert(apply_diff_to_value(entry.get(), diff));
                        }
                        Entry::Vacant(_) => panic!("Can't apply diff to nonexistent value {}", key)
                    }
                }

                bs::Value::Object(obj)
            } else {
                panic!("Can't apply an ObjectDiff to a non-object")
            }
        }
        bs::ValueDiff::ArrayDiff(changes) => {
            if let bs::Value::Array(arr) = value {
                let mut arr = arr.clone();
                for (i, diff) in changes {
                    arr[i] = apply_diff_to_value(&arr[i], diff);
                }

                bs::Value::Array(arr)
            } else {
                panic!("Can't apply an ArrayDiff to a non-array")
            }
        }
    }
}

fn json_to_state_value(value: &JsonValue, caused_by: Arc<bs::Event>) -> bs::Value {
    match value {
        JsonValue::Null => new_primitive_value(bs::PropertyValue::Null, caused_by),
        JsonValue::Bool(b) => new_primitive_value(bs::PropertyValue::Bool(*b), caused_by),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                new_primitive_value(bs::PropertyValue::Int(i), caused_by)
            } else if let Some(d) = n.as_f64() {
                new_primitive_value(bs::PropertyValue::Double(d), caused_by)
            } else {
                panic!("Couldn't represent number")
            }
        }
        JsonValue::String(s) => new_primitive_value(bs::PropertyValue::String(s.clone()), caused_by),
        JsonValue::Array(arr) => bs::Value::Array(
            arr.into_iter().map(|val| json_to_state_value(val, caused_by)).collect()
        ),
        JsonValue::Object(obj) => bs::Value::Object(
            obj.into_iter().map(|(key, val)| (key.clone(), json_to_state_value(val, caused_by))).collect()
        ),
    }
}