use std::collections::HashSet;
use std::rc::Rc;
use chrono::{DateTime, Utc};
use indenter::indented;
use serde_json::Value as JsonValue;
use log::debug;
use thiserror::Error;

use crate::blaseball_state::{BlaseballState, Event, KnownValue, PropertyValue, Uuid, Value as StateValue, Value};
use crate::ingest::chronicler::ChroniclerItem;
use crate::ingest::{IngestItem};
use crate::ingest::chronicler::error::{IngestError, UpdateMismatchError};

pub struct ChronUpdate {
    endpoint: &'static str,
    item: ChroniclerItem,
}


impl IngestItem for ChronUpdate {
    fn date(&self) -> DateTime<Utc> {
        self.update.valid_from
    }

    fn apply(&self, state: Rc<BlaseballState>) -> Result<Rc<BlaseballState>, IngestError> {
        todo!()
    }

}

pub fn apply_update(state: &Rc<BlaseballState>, endpoint_name: &str, entity_id: String, data: JsonValue) -> Result<(), UpdateMismatchError> {
    debug!("Applying update for {}", endpoint_name);
    let entity_state = &state.data[endpoint_name][&Uuid::new(entity_id)];
    apply_entity_update(entity_state, &data)
}

fn apply_entity_update(entity_state: &StateValue, entity_update: &JsonValue) -> Result<(), UpdateMismatchError> {
    match entity_state {
        Value::Object(state_obj) => {
            let update_obj = entity_update.as_object()
                .ok_or(UpdateMismatchError::TypeMismatch {
                    expected_type: "object".to_owned(),
                    actual_value: format!("{}", entity_update),
                })?;
            let state_keys: HashSet<_> = state_obj.keys().into_iter().collect();
            let update_keys: HashSet<_> = update_obj.keys().into_iter().collect();

            let extra_keys: Vec<_> = update_keys.difference(&state_keys).collect();
            if !extra_keys.is_empty() {
                return Err(UpdateMismatchError::ExtraKeys(
                    extra_keys.into_iter()
                        .map(|&key| (key.clone(), format!("{}", update_obj[key])))
                        .collect()
                ));
            }

            let missing_keys: Vec<String> = (&state_keys - &update_keys).iter().cloned().cloned().collect();
            if !missing_keys.is_empty() {
                return Err(UpdateMismatchError::MissingKeys(missing_keys));
            }

            let nested_errs: Vec<_> = update_obj.into_iter().filter_map(|(key, value)|
                match apply_entity_update(&state_obj[key], value) {
                    Ok(_) => None,
                    Err(e) => Some((key.to_string(), e))
                })
                .collect();

            if nested_errs.is_empty() {
                Ok(())
            } else {
                Err(UpdateMismatchError::NestedError { 0: nested_errs })
            }
        }
        Value::Array(state_arr) => {
            let update_arr = entity_update.as_array()
                .ok_or(UpdateMismatchError::TypeMismatch {
                    expected_type: "array".to_owned(),
                    actual_value: format!("{}", entity_update),
                })?;

            if state_arr.len() != update_arr.len() {
                return Err(UpdateMismatchError::ArraySizeMismatch {
                    expected: state_arr.len(),
                    actual: update_arr.len(),
                });
            }

            let nested_errs: Vec<(_, _)> = itertools::enumerate(itertools::zip(state_arr, update_arr))
                .filter_map(|(i, (state_item, update_item))|
                    match apply_entity_update(state_item, update_item) {
                        Ok(_) => None,
                        Err(e) => Some((format!("{}", i), e))
                    })
                .collect();

            if nested_errs.is_empty() {
                Ok(())
            } else {
                Err(UpdateMismatchError::NestedError { 0: nested_errs })
            }
        }
        Value::Value(state_val) => {
            match &state_val.value {
                PropertyValue::Known(known) => match known {
                    KnownValue::Null => {
                        entity_update.as_null()
                            .ok_or(UpdateMismatchError::TypeMismatch {
                                expected_type: "null".to_owned(),
                                actual_value: format!("{}", entity_update),
                            })
                    }
                    KnownValue::Bool(state_bool) => {
                        let entity_bool = entity_update.as_bool()
                            .ok_or(UpdateMismatchError::TypeMismatch {
                                expected_type: "bool".to_owned(),
                                actual_value: format!("{}", entity_update),
                            })?;

                        apply_value(state_bool, &entity_bool)
                    }
                    KnownValue::Int(state_int) => {
                        let entity_int = entity_update.as_i64()
                            .ok_or(UpdateMismatchError::TypeMismatch {
                                expected_type: "i64".to_owned(),
                                actual_value: format!("{}", entity_update),
                            })?;

                        apply_value(state_int, &entity_int)
                    }
                    KnownValue::Double(state_double) => {
                        let entity_double = entity_update.as_f64()
                            .ok_or(UpdateMismatchError::TypeMismatch {
                                expected_type: "f64".to_owned(),
                                actual_value: format!("{}", entity_update),
                            })?;

                        apply_value(state_double, &entity_double)
                    }
                    KnownValue::String(state_str) => {
                        let entity_str = entity_update.as_str()
                            .ok_or(UpdateMismatchError::TypeMismatch {
                                expected_type: "str".to_owned(),
                                actual_value: format!("{}", entity_update),
                            })?;

                        apply_value(&state_str.as_str(), &entity_str)
                    }
                    KnownValue::Deleted => todo!()
                }
                PropertyValue::Unknown(_) => todo!()
            }
        }
    }
}

fn apply_value<T: std::fmt::Display + std::cmp::PartialEq>(state_val: &T, entity_val: &T) -> Result<(), UpdateMismatchError> {
    if state_val != entity_val {
        Err(UpdateMismatchError::ValueMismatch {
            expected: format!("{}", state_val),
            actual: format!("{}", entity_val),
        })
    } else {
        Ok(())
    }
}