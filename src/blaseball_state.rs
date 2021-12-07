use std::fmt::{Debug, Display, Formatter, Write};
use im;
use uuid::Uuid;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use im::HashMap;
use itertools::Itertools;
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

impl Display for NodeValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeValue::Object(obj) => {
                if obj.is_empty() {
                    // Shortcut for two braces without spaces
                    return f.write_str("{}");
                }

                f.write_str("{ ")?;

                let mut first = true;
                for (key, node) in obj {
                    if !first {
                        first = false;
                        f.write_str(", ")?;
                    }

                    write!(f, "\"{}\": {}", key, node.value)?;
                }

                f.write_str(" }")
            }
            NodeValue::Array(arr) => {
                if arr.is_empty() {
                    // Shortcut for two brackets without spackets
                    return f.write_str("[]");
                }

                f.write_str("[ ")?;

                let mut first = true;
                for node in arr {
                    if !first {
                        first = false;
                        f.write_str(", ")?;
                    }

                    write!(f, "{}", node.value)?;
                }

                f.write_str(" ]")
            }
            NodeValue::Null => {
                write!(f, "null")
            }
            NodeValue::Bool(b) => {
                if *b {
                    f.write_str("true")
                } else {
                    f.write_str("false")
                }
            }
            NodeValue::Int(i) => {
                write!(f, "{}", i)
            }
            NodeValue::Float(d) => {
                write!(f, "{}", d)
            }
            NodeValue::String(s) => {
                f.write_str(s)
            }
            NodeValue::IntRange(lower, upper) => {
                write!(f, "<int between {} and {}>", lower, upper)
            }
            NodeValue::FloatRange(lower, upper) => {
                write!(f, "<float between {} and {}>", lower, upper)
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum PathError {
    #[error("Path error at {0}: Entity type does not exist")]
    EntityTypeDoesNotExist(&'static str),

    #[error("Path error at {0}/*: Tried to use a wildcard expression in a context that does not support it")]
    UnexpectedWildcard(&'static str),

    #[error("Path error at {0}/{1}: Entity does not exist")]
    EntityDoesNotExist(&'static str, Uuid),

    #[error("Path error at {path}: Expected {expected_type} but found {value}")]
    UnexpectedType { path: Path, expected_type: &'static str, value: String },

    #[error("Path error at {0}: Path does not exist")]
    MissingKey(Path),

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

impl Patch {
    pub fn description(&self, state: &BlaseballState) -> Result<String, PathError> {
        let str = match &self.change {
            ChangeType::Add(node) => {
                format!("{}: Add value {}", self.path, node.value)
            }
            ChangeType::Remove => {
                format!("{}: Remove value {}", self.path, state.node_at(&self.path)?.value)
            },
            ChangeType::Replace(node) => {
                format!("{}: Replace {} with {}", self.path, state.node_at(&self.path)?.value, node.value)
            },
            ChangeType::Increment => {
                format!("{}: Increment {}", self.path, state.node_at(&self.path)?.value)
            },
        };

        Ok(str)
    }
}

#[derive(Clone)]
pub enum ChangeType {
    Add(Arc<Node>),
    Remove,
    Replace(Arc<Node>),
    Increment,
}

#[derive(Clone, Debug)]
pub enum PathComponent {
    Key(String),
    Index(usize),
}

impl Display for PathComponent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PathComponent::Key(k) => { write!(f, "{}", k)?; }
            PathComponent::Index(i) => { write!(f, "{}", i)?; }
        }

        Ok(())
    }
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

#[derive(Clone, Debug)]
pub struct Path {
    pub entity_type: &'static str,
    // None means apply to all entities
    pub entity_id: Option<Uuid>,
    pub components: Vec<PathComponent>,
}

impl Path {
    pub fn slice(&self, index: usize) -> Path {
        Path {
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            components: self.components[0..(index+1)].to_vec(),
        }
    }
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

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(entity_id) = self.entity_id {
            write!(f, "{}/{}", self.entity_type, entity_id)?;
        } else {
            write!(f, "{}/*", self.entity_type)?;
        }

        for component in &self.components {
            write!(f, "/{}", component)?;
        }

        Ok(())
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

    pub fn node_at(&self, path: &Path) -> Result<&Node, PathError> {
        let entity_set = self.data.get(path.entity_type)
            .ok_or_else(|| PathError::EntityTypeDoesNotExist(path.entity_type))?;
        let entity_id = path.entity_id
            .ok_or_else(|| PathError::UnexpectedWildcard(path.entity_type))?;
        let entity = entity_set.get(&entity_id)
            .ok_or_else(|| PathError::EntityDoesNotExist(path.entity_type, entity_id))?;

        let mut node = entity;
        for (i, component) in path.components.iter().enumerate() {
            node = match &node.value {
                NodeValue::Object(obj) => {
                    match component {
                        PathComponent::Index(_) => {
                            Err(PathError::UnexpectedType {
                                path: path.slice(i),
                                expected_type: "object",
                                value: format!("{}", node.value)
                            })
                        }
                        PathComponent::Key(key) => {
                            obj.get(key)
                                .ok_or_else(|| PathError::MissingKey(path.slice(i)))
                        }
                    }
                }
                NodeValue::Array(arr) => {
                    match component {
                        PathComponent::Index(idx) => {
                            arr.get(*idx)
                                .ok_or_else(|| PathError::MissingKey(path.slice(i)))

                        }
                        PathComponent::Key(_) => {
                            Err(PathError::UnexpectedType {
                                path: path.slice(i),
                                expected_type: "array",
                                value: format!("{}", node.value)
                            })
                        }
                    }
                }
                _ => {
                    let expected_type = match component {
                        PathComponent::Index(_) => "array",
                        PathComponent::Key(_) => "object",
                    };

                    Err(PathError::UnexpectedType {
                        path: path.slice(i),
                        expected_type,
                        value: format!("{}", node.value)
                    })

                }
            }?;
        }

        Ok(node)
    }
}

fn apply_change(data: &mut HashMap<&str, EntitySet>, change: &Patch, caused_by: Arc<Event>) -> Result<(), PathError> {
    todo!()
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
