use im;
use uuid::Uuid;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use im::HashMap;
use serde_json::{Map, Value as JsonValue, Value};
use thiserror::Error;
use crate::ingest::{IngestError};

/// Describes the event that caused one BlaseballState to change into another BlaseballState
#[derive(Debug, Clone)]
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
pub struct Observation {
    pub entity_type: &'static str,
    pub entity_id: Uuid,
    pub observed_at: DateTime<Utc>,
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
pub type EntitySet = im::HashMap<Uuid, Arc<Node>>;

#[derive(Debug)]
pub struct Node {
    pub predecessor: Option<Arc<Node>>,
    pub caused_by: Arc<Event>,
    pub observed_by: Option<Observation>,
    pub value: NodeValue,
}

#[derive(Debug)]
pub enum NodeValue {
    // Deleted flag
    Deleted,

    // Collections
    Object(im::HashMap<String, Arc<Node>>),
    Array(im::Vector<Arc<Node>>),

    // Simple primitives
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),

    // Primitive placeholders
    IntRange(i64, i64),
    FloatRange(f64, f64),
}

#[derive(Error, Debug)]
pub enum ApplyPatchError {
    #[error("Chron {observation:?} update didn't match the expected value: {diff}")]
    UpdateMismatch { observation: Observation, diff: String },

    #[error("Expected {path} to have type {expected_type}, but it had type {actual_type}")]
    UnexpectedType { path: String, expected_type: &'static str, actual_type: &'static str },

    #[error("State was missing key {0}")]
    MissingKey(String),

}

impl Node {
    pub fn new(value: NodeValue, caused_by: Arc<Event>, observed_by: Option<Observation>) -> Arc<Node> {
        return Arc::new(Node {
            predecessor: None,
            caused_by,
            observed_by,
            value
        })
    }

    pub fn successor(self: &Arc<Self>, value: NodeValue, caused_by: Arc<Event>, observed_by: Option<Observation>) -> Arc<Node> {
        return Arc::new(Node {
            predecessor: Some(self.clone()),
            caused_by,
            observed_by,
            value
        })
    }

    pub fn new_from_json(value: &JsonValue, caused_by: Arc<Event>, observed_by: Option<Observation>) -> Arc<Node> {
        match value {
            JsonValue::Null => {
                Node::new(NodeValue::Null, caused_by, observed_by)
            }
            JsonValue::Bool(b) => {
                Node::new(NodeValue::Bool(*b), caused_by, observed_by)
            }
            JsonValue::Number(n) => {
                match n.as_i64() {
                    Some(i) => Node::new(NodeValue::Int(i), caused_by, observed_by),
                    None => {
                        let f = n.as_f64().expect("Number was neither i64 nor f64");
                        Node::new(NodeValue::Float(f), caused_by, observed_by)
                    }
                }
            }
            JsonValue::String(s) => {
                Node::new(NodeValue::String(s.clone()), caused_by, observed_by)
            }
            JsonValue::Array(arr) => {
                Node::new(NodeValue::new_from_json_array(arr, &caused_by, &observed_by),
                          caused_by, observed_by)
            }
            JsonValue::Object(obj) => {
                Node::new(NodeValue::new_from_json_object(obj, &caused_by, &observed_by),
                          caused_by, observed_by)
            }
        }
    }
}

impl NodeValue {
    pub fn new_from_json_object(obj: &Map<String, Value>, caused_by: &Arc<Event>, observed_by: &Option<Observation>) -> NodeValue {
        NodeValue::Object(
            obj.into_iter()
                .map(|(key, val)|
                    (key.clone(), Node::new_from_json(val, caused_by.clone(), observed_by.clone())))
                .collect()
        )
    }

    pub fn new_from_json_array(arr: &Vec<Value>, caused_by: &Arc<Event>, observed_by: &Option<Observation>) -> NodeValue {
        NodeValue::Array(
            arr.into_iter()
                .map(|val| Node::new_from_json(val, caused_by.clone(), observed_by.clone()))
                .collect()
        )
    }
}

#[derive(Clone)]
pub struct Patch {
    pub path: Path,
    pub change: ChangeType,
}

#[derive(Clone)]
pub enum ChangeType {
    Add(Arc<Node>),
    Remove,
    Replace(Arc<Node>),
    Increment,
}

#[derive(Clone)]
pub enum PathComponent {
    Key(String),
    Index(usize),
}

impl From<usize> for PathComponent {
    fn from(value: usize) -> PathComponent {
        PathComponent::Index(value)
    }
}

impl From<&'static str> for PathComponent {
    fn from(value: &'static str) -> PathComponent {
        PathComponent::Key(value.to_string())
    }
}

impl From<&String> for PathComponent {
    fn from(value: &String) -> PathComponent {
        PathComponent::Key(value.clone())
    }
}

impl From<String> for PathComponent {
    fn from(value: String) -> PathComponent {
        PathComponent::Key(value)
    }
}

#[derive(Clone)]
pub struct Path {
    pub entity_type: &'static str,
    // None means apply to all entities
    pub entity_id: Option<Uuid>,
    pub components: Vec<PathComponent>,
}

impl Path {
    pub fn extend(&self, end: PathComponent) -> Self {
        let mut components = self.components.clone();
        components.push(end);

        Self {
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            components
        }
    }
}

macro_rules! json_path {
    ($entity_type_expr:expr, $entity_id_expr:expr) => {{
        crate::blaseball_state::Path {
            entity_type: $entity_type_expr,
            entity_id: Some($entity_id_expr),
            components: vec![],
        }
    }};
    ($entity_type_expr:expr, $entity_id_expr:expr, $($x:expr),*) => {{
        let mut components: Vec<crate::blaseball_state::PathComponent> = Vec::new();
        $(
            components.push($x.into());
        )*

        crate::blaseball_state::Path {
            entity_type: $entity_type_expr,
            entity_id: Some($entity_id_expr),
            components
        }
    }};
}

pub(crate) use json_path;

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

    pub fn successor(self: Arc<Self>, event: Event, patches: Vec<Patch>) -> Result<Arc<BlaseballState>, IngestError> {
        let mut new_data = self.data.clone();

        let caused_by = Arc::new(event);

        for patch in patches {
            apply_change(&mut new_data, &patch, caused_by.clone())?;
        }

        Ok(Arc::new(BlaseballState {
            predecessor: Some(self),
            from_event: caused_by,
            data: new_data,
        }))
    }
}

fn apply_change(data: &mut HashMap<&str, EntitySet>, change: &Patch, caused_by: Arc<Event>) -> Result<(), ApplyPatchError> {
    todo!()
}

fn get_value_ref(value: &mut Node, path: &[PathComponent], change: &ChangeType, caused_by: Arc<Event>, path_str: String) -> Result<(), ApplyPatchError> {
    todo!()

/*    return match (value, path.first()) {
        (_, None) => {
            *value = change.
        }
        (Node::Object(obj), Some(PathComponent::Key(key))) => {
            let value = obj.get_mut(*key)
                .ok_or(ApplyChangeError::MissingKey(format!("{}/{}", path_str, key)))?;
            get_value_ref(value, &path[1..], change, caused_by, format!("{}/{}", path_str, key))
        }
        (Node::Array(arr), Some(PathComponent::Index(i))) => {
            let value = arr.get_mut(*i)
                .ok_or(ApplyChangeError::MissingKey(format!("{}/{}", path_str, i)))?;
            get_value_ref(value, &path[1..], change, caused_by, format!("{}/{}", path_str, i))
        }

    }



    match path.split_first() {
        None => Err(ApplyChangeError::MissingKey(path_str)),
        Some((first, rest)) => {
            match value {
                Node::Object(obj) => {
                    match first {
                        PathComponent::Key(k) => {
                            let path_str = format!("{}/{}", path_str, k);
                            match obj.get_mut(*k) {
                                None => Err(ApplyChangeError::MissingKey(path_str)),
                                Some(value) => get_value_ref(value, rest, change, caused_by, path_str)
                            }
                        }
                        PathComponent::Index(i) => {
                            Err(ApplyChangeError::UnexpectedType {
                                path: format!("{}/{}", path_str, i),
                                expected_type: "object",
                                actual_type: "array",
                            })
                        }
                    }
                }
                Node::Array(arr) => {
                    match first {
                        PathComponent::Key(k) => {
                            Err(ApplyChangeError::UnexpectedType {
                                path: format!("{}/{}", path_str, k),
                                expected_type: "array",
                                actual_type: "object",
                            })
                        }
                        PathComponent::Index(i) => {
                            let path_str = format!("{}/{}", path_str, i);
                            match arr.get_mut(*i) {
                                None => Err(ApplyChangeError::MissingKey(path_str)),
                                Some(value) => {
                                    if rest.is_empty() {
                                        *value = Node::Primitive(Arc::new(TrackedValue {
                                            predecessor: value,
                                            caused_by,
                                            observed_by: None,
                                            value: PrimitiveValue::Null
                                        }))
                                    } else {
                                        get_value_ref(value, rest, change, caused_by, path_str)
                                    }
                                }
                            }
                        }
                    }
                }
                Node::Primitive(_) => {
                    match first {
                        PathComponent::Key(k) => {
                            Err(ApplyChangeError::UnexpectedType {
                                path: format!("{}/{}", path_str, k),
                                expected_type: "array",
                                actual_type: "primitive",
                            })
                        }
                        PathComponent::Index(i) => {
                            Err(ApplyChangeError::UnexpectedType {
                                path: format!("{}/{}", path_str, i),
                                expected_type: "object",
                                actual_type: "primitive",
                            })
                        }
                    }
                }
            }
        }
    }

 */
}

fn records_from_chron_at_time(entity_type: &'static str, at_time: &'static str) -> impl Iterator<Item=(Uuid, Arc<Node>)> {
    let event = Arc::new(Event::Start);
    crate::api::chronicler::entities(entity_type, at_time)
        .map(move |item| {
            let obs = Observation {
                entity_type,
                entity_id: item.entity_id,
                observed_at: item.valid_from,
            };

            (obs.entity_id.clone(), Node::new_from_json(&item.data, event.clone(), Some(obs)))
        })
}
