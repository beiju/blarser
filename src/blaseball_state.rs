use std::collections;
use im;
use uuid::Uuid;
use std::sync::Arc;
use indenter::indented;
use std::fmt::Write;
use chrono::{DateTime, Utc};
use im::HashMap;
use serde_json as json;
use crate::ingest::{IngestError, IngestResult};

#[derive(Debug, Clone)]
pub struct Observation {
    pub entity_type: &'static str,
    pub entity_id: Uuid,
    pub observed_at: DateTime<Utc>,
}

/// Describes the event that caused one BlaseballState to change into another BlaseballState
#[derive(Debug)]
pub enum Event {
    /// A special event that should only be associated with the first BlaseballState. Represents
    /// Blaseball coming into existence.
    Start,

    /// Represents a change that was derived directly from an observation. Implicit changes have to
    /// be manually approved.
    ImplicitChange(Observation),

    FeedEvent(Uuid),
}
#[derive(Debug, Clone)]
pub struct BlaseballState {
    pub predecessor: Option<Arc<BlaseballState>>,
    pub from_event: Arc<Event>,
    pub data: BlaseballData,
}

// The top levels of the state need to be handled directly, because they're separate objects in
// Chron.
pub type BlaseballData = im::HashMap<&'static str, EntitySet>;
pub type EntitySet = im::HashMap<Uuid, Value>;


#[derive(Debug, Clone)]
pub enum Value {
    Object(im::HashMap<String, Value>),
    Array(im::Vector<Value>),
    Value(Arc<TrackedValue>),
}

#[derive(Debug)]
pub struct TrackedValue {
    pub predecessor: Option<Arc<TrackedValue>>,
    pub caused_by: Arc<Event>,
    pub observed_by: Option<Observation>,
    pub value: PropertyValue,
}

#[derive(Debug)]
pub enum PropertyValue {
    // Known values
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    Deleted,

    // Partially-known values
    IntRange(i64, i64),
    DoubleRange(f64, f64),

    // Unknown values (TODO)
}

impl Value {
    pub fn set_value(&mut self, path: &[PathComponent], value: serde_json::Value) -> Result<(), IngestError> {
        match path {
            [PathComponent::Key(key)] => {
                match self {
                    Value::Object(_) => {
                        Err(IngestError::UnexpectedType { path: key.to_string(), expected_type: "value", actual_type: "object" })
                    }
                    Value::Array(_) => {
                        Err(IngestError::UnexpectedType { path: key.to_string(), expected_type: "value", actual_type: "array" })
                    }
                    Value::Value(value) => {
                        Err(IngestError::UnexpectedType { path: key.to_string(), expected_type: "value", actual_type: "value" })
                    }
                }
            }
            [PathComponent::Index(i)] => {
                match self {
                    Value::Object(_) => {
                        Err(IngestError::UnexpectedType { path: i.to_string(), expected_type: "value", actual_type: "object" })
                    }
                    Value::Array(arr) => {
                        Err(IngestError::UnexpectedType { path: i.to_string(), expected_type: "value", actual_type: "array" })
                    }
                    Value::Value(_) => {
                        Err(IngestError::UnexpectedType { path: i.to_string(), expected_type: "value", actual_type: "value" })
                    }
                }
            }
            [PathComponent::Key(key), rest @ ..] => {
                match self {
                    Value::Object(obj) => {
                        match obj[*key].set_value(rest, value) {
                            Err(IngestError::UnexpectedType { path: nested_path, expected_type, actual_type }) => {
                                Err(IngestError::UnexpectedType {
                                    path: format!("{}/{}", key, nested_path),
                                    expected_type,
                                    actual_type,
                                })
                            }
                            x => x
                        }
                    }
                    Value::Array(_) => {
                        Err(IngestError::UnexpectedType { path: key.to_string(), expected_type: "object", actual_type: "array" })
                    }
                    Value::Value(_) => {
                        Err(IngestError::UnexpectedType { path: key.to_string(), expected_type: "object", actual_type: "value" })
                    }
                }
            }
            [PathComponent::Index(i), rest @ ..] => {
                match self {
                    Value::Object(_) => {
                        Err(IngestError::UnexpectedType { path: i.to_string(), expected_type: "array", actual_type: "object" })
                    }
                    Value::Array(arr) => {
                        match arr[*i].set_value(rest, value) {
                            Err(IngestError::UnexpectedType { path: nested_path, expected_type, actual_type }) => {
                                Err(IngestError::UnexpectedType {
                                    path: format!("{}/{}", i, nested_path),
                                    expected_type,
                                    actual_type,
                                })
                            }
                            x => x
                        }
                    }
                    Value::Value(_) => {
                        Err(IngestError::UnexpectedType { path: i.to_string(), expected_type: "array", actual_type: "value" })
                    }
                }
            }
        }
    }
}

impl PropertyValue {
    fn from_json(val: serde_json::Value) -> PropertyValue {
        match val {
            serde_json::Value::Null => PropertyValue::Null,
            serde_json::Value::Bool(b) => PropertyValue::Bool(b),
            serde_json::Value::Number(n) => {
                n.as_i64().map(|i| PropertyValue::Int(i))
                    .or(n.as_f64().map(|f| PropertyValue::Double(f)))
                    .expect("Invalid number")
            }
            serde_json::Value::String(s) => PropertyValue::String(s),
            _ => panic!("Tried to store composite value in PropertyValue")
        }
    }
}

pub enum ValueChange {
    SetValue {
        path: Path,
        value: serde_json::Value,
    }
}

pub enum PathComponent {
    Key(&'static str),
    Index(usize),
}

impl From<usize> for PathComponent {
    fn from(value: usize) -> PathComponent {
        PathComponent::Index(value)
    }
}

impl From<&'static str> for PathComponent {
    fn from(value: &'static str) -> PathComponent {
        PathComponent::Key(value)
    }
}

pub struct Path {
    pub entity_type: &'static str,
    pub entity_id: Uuid,
    pub components: Vec<PathComponent>,
}

macro_rules! json_path {
    ($entity_type_expr:expr, $entity_id_expr:expr, $($x:expr),*) => {{
        let mut components: Vec<crate::blaseball_state::PathComponent> = Vec::new();
        $(
            components.push($x.into());
        )*

        crate::blaseball_state::Path {
            entity_type: $entity_type_expr,
            entity_id: $entity_id_expr,
            components
        }
    }}
}

pub(crate) use json_path;

impl Event {
    pub fn new_implicit_change(observation: Observation) -> Event {
        return Event::ImplicitChange(observation);
    }
}

impl BlaseballState {
    pub fn from_chron_at_time(at_time: &'static str) -> BlaseballState {
        // Start all the endpoints first
        let endpoints: Vec<_> = crate::api::chronicler::ENDPOINT_NAMES.into_iter().map(|endpoint_name|
            (endpoint_name, records_from_chron_at_time(endpoint_name, at_time))).collect();

        BlaseballState {
            predecessor: None,
            from_event: Arc::new(Event::Start),
            data: endpoints.into_iter().map(|(endpoint_name, endpoint_iter)|
                (endpoint_name, endpoint_iter.collect())
            ).collect(),
        }
    }

    pub fn successor(self: Arc<Self>, event: Event, changes: Vec<ValueChange>) -> Result<Arc<BlaseballState>, IngestError> {
        let mut new_data = self.data.clone();

        let caused_by = Arc::new(event);

        for change in changes {
            apply_change(&mut new_data, change, caused_by.clone())?;
        }

        Ok(Arc::new(BlaseballState {
            predecessor: Some(self),
            from_event: caused_by,
            data: new_data,
        }))
    }
}

fn apply_change(data: &mut HashMap<&str, EntitySet>, change: ValueChange, caused_by: Arc<Event>) -> Result<BlaseballData, IngestError> {
    match change {
        ValueChange::SetValue { path, value: new_value } => {
            let mut path_str = format!("{}", path.entity_type);
            let data_for_type = data.get_mut(path.entity_type)
                .ok_or(IngestError::MissingKey(path_str))?;

            path_str = format!("{}/{}", path_str, path.entity_type);
            let mut value = data_for_type.get_mut(&path.entity_id)
                .ok_or(IngestError::MissingKey(path_str))?;

            for component in path.components {
                value = match component {
                    PathComponent::Key(key) => {
                        path_str = format!("{}/{}", path_str, key);
                        match value {
                            Value::Object(obj) => {
                                obj.get_mut(key).ok_or(IngestError::MissingKey(path_str))
                            }
                            Value::Array(_) => {
                                Err(IngestError::UnexpectedType { path: path_str, expected_type: "object", actual_type: "array"})
                            }
                            Value::Value(_) => {
                                Err(IngestError::UnexpectedType { path: path_str, expected_type: "object", actual_type: "value"})
                            }
                        }
                    }
                    PathComponent::Index(i) => {
                        path_str = format!("{}/{}", path_str, i);
                        match value {
                            Value::Object(_) => {
                                Err(IngestError::UnexpectedType { path: path_str, expected_type: "array", actual_type: "object"})
                            }
                            Value::Array(arr) => {
                                arr.get_mut(i).ok_or(IngestError::MissingKey(path_str))
                            }
                            Value::Value(_) => {
                                Err(IngestError::UnexpectedType { path: path_str, expected_type: "array", actual_type: "value"})
                            }
                        }

                    }
                }?;
            }

            // This block ensures that the structure doesn't change
            *value = match value {
                Value::Object(_) => {
                    Err(IngestError::UnexpectedType { path: path_str, expected_type: "value", actual_type: "object"})
                }
                Value::Array(_) => {
                    Err(IngestError::UnexpectedType { path: path_str, expected_type: "value", actual_type: "array"})
                }
                Value::Value(prev_val) => {
                    Ok(Value::Value(Arc::new(TrackedValue {
                        predecessor: Some(prev_val.clone()),
                        caused_by,
                        observed_by: None,
                        value: PropertyValue::from_json(new_value)
                    })))
                }
            }?;
        }
    }

    todo!()
}

fn records_from_chron_at_time(entity_type: &'static str, at_time: &'static str) -> impl Iterator<Item=(Uuid, Value)> {
    crate::api::chronicler::entities(entity_type, at_time)
        .map(move |item| {
            let obs = Observation {
                entity_type,
                entity_id: item.entity_id,
                observed_at: item.valid_from
            };

            (obs.entity_id.clone(), node_from_json(item.data, obs))
        })
}

fn node_from_json(value: json::Value, obs: Observation) -> Value {
    match value {
        json::Value::Null => root_property(PropertyValue::Null, obs),
        json::Value::Bool(b) => root_property(PropertyValue::Bool(b), obs),
        json::Value::Number(n) => match n.as_i64() {
            Some(i) => root_property(PropertyValue::Int(i), obs),
            None => root_property(PropertyValue::Double(n.as_f64().unwrap()), obs)
        },
        json::Value::String(s) => root_property(PropertyValue::String(s), obs),
        json::Value::Array(arr) => Value::Array(
            arr.into_iter().map(|item| node_from_json(item, obs)).collect()
        ),
        json::Value::Object(obj) => Value::Object(
            obj.into_iter().map(|(key, item)| (key, node_from_json(item, obs))).collect()
        ),
    }
}

fn root_property(value: PropertyValue, observation: Observation) -> Value {
    Value::Value(Arc::new(TrackedValue {
        predecessor: None,
        caused_by: Arc::new(Event::Start),
        observed_by: Some(observation),
        value,
    }))
}