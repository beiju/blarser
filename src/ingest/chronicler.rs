use std::collections;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::rc::Rc;
use chrono::{DateTime, Utc};
use log::{debug, error};
use serde_json::{Value as JsonValue};
use dialoguer::Confirm;
use im::hashmap::Entry;


use crate::api::{chronicler, ChroniclerItem};
use crate::blaseball_state as bs;
use crate::blaseball_state::{BlaseballState, KnownValue, PropertyValue, TrackedValue, Value, ValueDiff};
use crate::ingest::IngestItem;
use crate::ingest::error::IngestError;

pub struct ChronUpdate {
    endpoint: &'static str,
    item: ChroniclerItem,
}

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=Box<dyn IngestItem>>>> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |endpoint|
            Box::new(chronicler::versions(endpoint, start)
                .map(|item| Box::new(ChronUpdate { endpoint, item }) as Box<dyn IngestItem>))
            as Box<dyn Iterator<Item = Box<(dyn IngestItem)>>>
        )
        .collect()
}

impl IngestItem for ChronUpdate {
    fn date(&self) -> DateTime<Utc> {
        self.item.valid_from
    }

    fn apply(self: Box<Self>, state: Rc<bs::BlaseballState>) -> Result<Rc<bs::BlaseballState>, IngestError> {
        let endpoint = self.endpoint;
        let entity_id = bs::Uuid::new(self.item.entity_id.clone());

        match apply_update(&state, endpoint, &entity_id, &self.item.data) {
            Ok(()) => Ok(state),
            Err(e) => {
                try_structural_update(state.clone(), endpoint, entity_id, e)
                    .and_then(|state| self.apply(state))

            }
        }
    }
}

fn try_structural_update(state: Rc<BlaseballState>, endpoint: &'static str, entity_id: bs::Uuid, diff: ValueDiff) -> Result<Rc<BlaseballState>, IngestError> {
    if diff.is_valid_structural_update() {
        let prompt = format!("Change found in {}: {}\nApply this as a structural update?", endpoint, diff);
        if Confirm::new().with_prompt(prompt).interact()? {
            return Ok(apply_structural_update(state, endpoint, entity_id, diff))
        }
    }

    Err(IngestError::UpdateMismatch {endpoint, diff: format!("{}", diff) })
}

fn apply_structural_update(state: Rc<BlaseballState>, endpoint: &'static str, entity_id: bs::Uuid, diff: ValueDiff) -> Rc<BlaseballState> {
    let new_data = state.data.alter(|entity_set|
        match entity_set {
            Some(entity_set) => Some(entity_set.alter(|entity| match entity {
                Some(data) => Some(apply_structural_update_to_value(&data, diff)),
                None => panic!("Tried to apply structural update to nonexistent object")
            }, entity_id)),
            None => panic!("Tried to apply structural update to nonexistent endpoint")
        }, endpoint);

    Rc::new(BlaseballState {
        predecessor: Some(state),
        from_event: Rc::new(bs::Event::StructuralUpdate {endpoint}),
        data: new_data
    })
}

fn new_primitive_value(val: KnownValue) -> Value {
    Value::Value(Rc::new(TrackedValue {
        predecessor: None,
        value: PropertyValue::Known(val),
    }))
}

fn apply_structural_update_to_value(value: &Value, diff: ValueDiff) -> Value {
    match diff {
        ValueDiff::KeysRemoved(keys) => {
            if let Value::Object(obj) = value {
                let mut obj = obj.clone();
                for key in keys {
                    obj.remove(&*key);
                }
                Value::Object(obj)
            } else {
                panic!("Can't apply a KeysRemoved diff to a non-object");
            }
        }
        ValueDiff::KeysAdded(children) => {
            if let Value::Object(obj) = value {
                let mut obj = obj.clone();
                for (key, value) in children {
                    obj.insert(key, json_to_state_value(value));
                }

                Value::Object(obj)
            } else {
                panic!("Can't apply a KeysRemoved diff to a non-object");
            }
        }
        ValueDiff::ArraySizeChanged { after, .. } => {
            if let Value::Array(_) = value {
                if after == 0 {
                    Value::Array(im::Vector::new())
                } else {
                    panic!("Can't apply a non-0 ArraySizeChanged diff as a structural update")
                }
            } else {
                panic!("Can't apply an ArraySizeChanged diff to a non-array")
            }
        }
        ValueDiff::ValueChanged { after, .. } => json_to_state_value(after),
        ValueDiff::ObjectDiff(changes) => {
            if let Value::Object(obj) = value {
                let mut obj = obj.clone();
                for (key, diff) in changes {
                    match obj.entry(key.clone()) {
                        Entry::Occupied(mut entry) => {
                            entry.insert(apply_structural_update_to_value(entry.get(), diff));
                        }
                        Entry::Vacant(_) => panic!("Can't apply diff to nonexistent value {}", key)
                    }
                }

                Value::Object(obj)
            } else {
                panic!("Can't apply an ObjectDiff to a non-object")
            }
        }
        ValueDiff::ArrayDiff(changes) => {
            if let Value::Array(arr) = value {
                let mut arr = arr.clone();
                for (i, diff) in changes {
                    arr[i] = apply_structural_update_to_value(&arr[i], diff);
                }

                Value::Array(arr)
            } else {
                panic!("Can't apply an ArrayDiff to a non-array")
            }
        }
    }
}

fn json_to_state_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => new_primitive_value(KnownValue::Null),
        JsonValue::Bool(b) => new_primitive_value(KnownValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                new_primitive_value(KnownValue::Int(i))
            } else if let Some(d) = n.as_f64() {
                new_primitive_value(KnownValue::Double(d))
            } else {
                panic!("Couldn't represent number")
            }
        },
        JsonValue::String(s) => new_primitive_value(KnownValue::String(s.clone())),
        JsonValue::Array(arr) => Value::Array(
            arr.into_iter().map(json_to_state_value).collect()
        ),
        JsonValue::Object(obj) => Value::Object(
            obj.into_iter().map(|(key, val)| (key.clone(), json_to_state_value(val))).collect()
        ),
    }
}

pub fn apply_update<'a>(state: &'a Rc<bs::BlaseballState>, endpoint_name: &str, entity_id: &bs::Uuid, data: &'a JsonValue) -> Result<(), ValueDiff<'a>> {
    debug!("Applying Chron {} update", endpoint_name);
    let entity_state = &state.data[endpoint_name][entity_id];
    apply_entity_update(entity_state, &data)
}

fn apply_entity_update<'a>(entity_state: &'a bs::Value, entity_update: &'a JsonValue) -> Result<(), ValueDiff<'a>> {
    match entity_state {
        bs::Value::Object(state_obj) => {
            let update_obj = entity_update.as_object()
                .ok_or(ValueDiff::ValueChanged {
                    before: entity_state,
                    after: entity_update,
                })?;
            let state_keys: HashSet<_> = state_obj.keys().into_iter().collect();
            let update_keys: HashSet<_> = update_obj.keys().into_iter().collect();

            let missing_keys: Vec<String> = (&state_keys - &update_keys).iter().cloned().cloned().collect();
            if !missing_keys.is_empty() {
                return Err(ValueDiff::KeysRemoved(missing_keys));
            }

            let extra_keys: Vec<_> = update_keys.difference(&state_keys).collect();
            if !extra_keys.is_empty() {
                return Err(ValueDiff::KeysAdded(
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
                Err(ValueDiff::ObjectDiff { 0: nested_errs })
            }
        }
        bs::Value::Array(state_arr) => {
            let update_arr = entity_update.as_array()
                .ok_or(ValueDiff::ValueChanged {
                    before: entity_state,
                    after: entity_update,
                })?;

            if state_arr.len() != update_arr.len() {
                return Err(ValueDiff::ArraySizeChanged {
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
                Err(ValueDiff::ArrayDiff { 0: nested_errs })
            }
        }
        bs::Value::Value(state_val) => {
            match &state_val.value {
                bs::PropertyValue::Known(known) => match known {
                    bs::KnownValue::Null => {
                        entity_update.as_null()
                            .ok_or(ValueDiff::ValueChanged {
                                before: entity_state,
                                after: entity_update,
                            })
                    }
                    bs::KnownValue::Bool(state_bool) => {
                        entity_update.as_bool()
                            .filter(|v| state_bool == v )
                            .ok_or(ValueDiff::ValueChanged {
                                before: entity_state,
                                after: entity_update,
                            })
                            .map(|_| ())

                    }
                    bs::KnownValue::Int(state_int) => {
                        entity_update.as_i64()
                            .filter(|v| state_int == v )
                            .ok_or(ValueDiff::ValueChanged {
                                before: entity_state,
                                after: entity_update,
                            })
                            .map(|_| ())
                    }
                    bs::KnownValue::Double(state_double) => {
                        entity_update.as_f64()
                            .filter(|v| state_double == v )
                            .ok_or(ValueDiff::ValueChanged {
                                before: entity_state,
                                after: entity_update,
                            })
                            .map(|_| ())
                    }
                    bs::KnownValue::String(state_str) => {
                        entity_update.as_str()
                            .filter(|v| state_str == v )
                            .ok_or(ValueDiff::ValueChanged {
                                before: entity_state,
                                after: entity_update,
                            })
                            .map(|_| ())
                    }
                    bs::KnownValue::Deleted => todo!()
                }
                bs::PropertyValue::Unknown(_) => todo!()
            }
        }
    }
}