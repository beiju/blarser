use std::fmt::{Debug, Display, Formatter};
use std::iter;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use uuid::Uuid;
use std::sync::{Arc, RwLock};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value as JsonValue, Value};
use thiserror::Error;

use crate::ingest::IngestError;

/// Describes the event that caused one BlaseballState to change into another BlaseballState
#[derive(Debug, Clone)]
pub enum Event {
    /// A special event that should only be associated with the first BlaseballState. Represents
    /// Blaseball coming into existence.
    Start,

    /// Represents a change that was derived directly from an observation. Implicit changes have to
    /// be manually approved.
    ImplicitChange(Observation),

    /// Represents a change derived from a Feed event. This is the most common case.
    FeedEvent(Uuid),

    /// Represents a change that automatically happens at a certain time.
    TimedChange(DateTime<Utc>),
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
pub type EntitySet = im::HashMap<Uuid, Node>;

#[derive(Debug, Clone)]
pub enum Node {
    Object(im::HashMap<String, Node>),
    Array(im::Vector<Node>),
    Primitive(SharedPrimitiveNode),
}

#[derive(Debug, Clone)]
pub struct SharedPrimitiveNode(Arc<RwLock<PrimitiveNode>>);

impl SharedPrimitiveNode {
    pub fn set<T: Into<PrimitiveValue>>(&mut self, value: T, caused_by: Arc<Event>) {
        *self = self.successor(value, caused_by).into();
    }

    fn successor<T: Into<PrimitiveValue>>(&self, value: T, caused_by: Arc<Event>) -> PrimitiveNode {
        let observed_by = if let Event::ImplicitChange(observation) = &*caused_by {
            Some(observation.clone())
        } else {
            None
        };

        PrimitiveNode {
            predecessor: Some(self.clone()),
            caused_by,
            observed_by,
            value: value.into(),
        }
    }
}

impl Deref for SharedPrimitiveNode {
    type Target = Arc<RwLock<PrimitiveNode>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SharedPrimitiveNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<PrimitiveNode> for SharedPrimitiveNode {
    fn from(primitive_node: PrimitiveNode) -> Self {
        SharedPrimitiveNode(Arc::new(RwLock::new(primitive_node)))
    }
}

#[derive(Debug)]
pub struct PrimitiveNode {
    pub predecessor: Option<SharedPrimitiveNode>,
    pub caused_by: Arc<Event>,
    pub observed_by: Option<Observation>,
    pub value: PrimitiveValue,
}

#[derive(Clone, Debug)]
pub enum PrimitiveValue {
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

impl Display for PrimitiveValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveValue::Null => {
                write!(f, "null")
            }
            PrimitiveValue::Bool(b) => {
                if *b {
                    f.write_str("true")
                } else {
                    f.write_str("false")
                }
            }
            PrimitiveValue::Int(i) => {
                write!(f, "{}", i)
            }
            PrimitiveValue::Float(d) => {
                write!(f, "{}", d)
            }
            PrimitiveValue::String(s) => {
                write!(f, "\"{}\"", s)
            }
            PrimitiveValue::IntRange(lower, upper) => {
                write!(f, "<int between {} and {}>", lower, upper)
            }
            PrimitiveValue::FloatRange(lower, upper) => {
                write!(f, "<float between {} and {}>", lower, upper)
            }
        }
    }
}

impl PartialEq for PrimitiveValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(a), Self::Bool(b)) => a == b,
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::String(a), Self::String(b)) => a == b,
            _ => false
        }
    }
}

impl From<i64> for PrimitiveValue {
    fn from(i: i64) -> Self {
        PrimitiveValue::Int(i)
    }
}

impl From<bool> for PrimitiveValue {
    fn from(b: bool) -> Self {
        PrimitiveValue::Bool(b)
    }
}

impl From<String> for PrimitiveValue {
    fn from(s: String) -> Self {
        PrimitiveValue::String(s)
    }
}

impl From<Uuid> for PrimitiveValue {
    fn from(u: Uuid) -> Self {
        PrimitiveValue::String(u.to_string())
    }
}

impl From<&'static str> for PrimitiveValue {
    fn from(s: &'static str) -> Self {
        PrimitiveValue::String(s.into())
    }
}

impl PrimitiveValue {
    pub fn equals<T: Into<Self>>(&self, other: T) -> bool {
        &other.into() == self
    }

    pub fn from_json_number(n: &serde_json::Number) -> PrimitiveValue {
        match n.as_i64() {
            Some(i) => PrimitiveValue::Int(i),
            None => {
                let f = n.as_f64().expect("Number was neither i64 nor f64");
                PrimitiveValue::Float(f)
            }
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            PrimitiveValue::Null => { true }
            _ => { false }
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PrimitiveValue::Bool(b) => { Some(*b) }
            _ => { None }
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            PrimitiveValue::Int(i) => { Some(*i) }
            _ => { None }
        }
    }

    pub fn as_int_range(&self) -> Option<(i64, i64)> {
        match self {
            PrimitiveValue::IntRange(lower, upper) => { Some((*lower, *upper)) }
            _ => { None }
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            PrimitiveValue::Float(f) => { Some(*f) }
            _ => { None }
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            PrimitiveValue::String(s) => { Some(s) }
            _ => { None }
        }
    }

    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            PrimitiveValue::String(s) => { Uuid::from_str(s).ok() }
            _ => { None }
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

    #[error("Path error at {path}: Expected {expected_type} but found {value:#}")]
    UnexpectedType { path: Path, expected_type: &'static str, value: String },

    #[error("Path error at {0}: Path does not exist")]
    MissingKey(Path),

    #[error(transparent)]
    UuidError(#[from] uuid::Error),
}

#[derive(Error, Debug)]
pub enum ApplyChangeError {
    #[error("Expected nothing at {0} but found {1}")]
    UnexpectedValue(Path, String),

    #[error("Expected value at {0} but found nothing")]
    MissingValue(Path),

    #[error(transparent)]
    PathError {
        #[from]
        from: PathError
    },

}

impl Node {
    pub fn to_string(&self) -> String {
        match self {
            Node::Object(obj) => {
                Self::object_to_string(obj)
            }
            Node::Array(arr) => {
                Self::array_to_string(arr)
            }
            Node::Primitive(primitive) => {
                Self::primitive_to_string(primitive)
            }
        }
    }

    pub fn as_array(&self) -> Result<&im::Vector<Node>, String> {
        match self {
            Node::Array(arr) => { Ok(arr) }
            Node::Object(_) => { Err("object".to_string()) }
            Node::Primitive(_) => { Err("primitive".to_string()) }
        }
    }

    pub fn as_array_mut(&mut self) -> Result<&mut im::Vector<Node>, String> {
        match self {
            Node::Array(arr) => { Ok(arr) }
            Node::Object(_) => { Err("object".to_string()) }
            Node::Primitive(_) => { Err("primitive".to_string()) }
        }
    }

    pub fn as_object(&self) -> Result<&im::HashMap<String, Node>, String> {
        match self {
            Node::Object(obj) => { Ok(obj) }
            Node::Array(_) => { Err("array".to_string()) }
            Node::Primitive(_) => { Err("primitive".to_string()) }
        }
    }

    pub fn as_object_mut(&mut self) -> Result<&mut im::HashMap<String, Node>, String> {
        match self {
            Node::Object(obj) => { Ok(obj) }
            Node::Array(_) => { Err("array".to_string()) }
            Node::Primitive(_) => { Err("primitive".to_string()) }
        }
    }

    pub fn as_primitive(&self) -> Result<&SharedPrimitiveNode, String> {
        match self {
            Node::Primitive(p) => { Ok(p) }
            Node::Object(_) => { Err("object".to_string()) }
            Node::Array(_) => { Err("array".to_string()) }
        }
    }

    pub fn as_primitive_mut(&mut self) -> Result<&mut SharedPrimitiveNode, String> {
        match self {
            Node::Primitive(p) => { Ok(p) }
            Node::Object(_) => { Err("object".to_string()) }
            Node::Array(_) => { Err("array".to_string()) }
        }
    }

    pub fn as_int(&self) -> Result<i64, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().unwrap();
                primitive.value.as_int()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string()) }
        }
    }

    pub fn as_int_range(&self) -> Result<(i64, i64), String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().unwrap();
                primitive.value.as_int_range()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string()) }
        }
    }

    pub fn as_bool(&self) -> Result<bool, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().unwrap();
                primitive.value.as_bool()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string()) }
        }
    }

    pub fn as_string(&self) -> Result<String, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().unwrap();
                primitive.value.as_str().map(|s| s.to_string())
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string()) }
        }
    }

    pub fn as_uuid(&self) -> Result<Uuid, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().unwrap();
                primitive.value.as_uuid()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string()) }
        }
    }


    pub fn primitive_to_string(primitive: &SharedPrimitiveNode) -> String {
        let lock = primitive.read().unwrap();
        format!("{}", lock.value)
    }

    pub fn array_to_string(arr: &im::Vector<Node>) -> String {
        let inner = arr.iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        format!("[{}]", inner)
    }

    pub fn object_to_string<T: Clone + std::hash::Hash + std::cmp::Eq + Display>(obj: &im::HashMap<T, Node>) -> String
        where T: Clone + std::hash::Hash + std::cmp::Eq + Display {
        if obj.is_empty() {
            // Shortcut for two braces without spaces
            return "{}".to_string();
        }

        let inner = obj.iter()
            .map(|(key, node)| {
                format!("\"{}\": {}", key, node.to_string())
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!("{{ {} }}", inner)
    }

    pub fn new_primitive(value: PrimitiveValue, caused_by: Arc<Event>) -> Node {
        let observed_by = match &*caused_by {
            Event::ImplicitChange(observation) => Some(observation.clone()),
            _ => None
        };

        Node::Primitive(PrimitiveNode {
            predecessor: None,
            caused_by,
            observed_by,
            value,
        }.into())
    }

    pub fn primitive_successor(predecessor: SharedPrimitiveNode, value: PrimitiveValue, caused_by: Arc<Event>) -> Node {
        let observed_by = match &*caused_by {
            Event::ImplicitChange(observation) => Some(observation.clone()),
            _ => None
        };

        Node::Primitive(PrimitiveNode {
            predecessor: Some(predecessor),
            caused_by,
            observed_by,
            value,
        }.into())
    }

    pub fn successor_from_primitive(predecessor: SharedPrimitiveNode, value: JsonValue, caused_by: Arc<Event>) -> Node {
        match value {
            Value::Null => {
                Node::primitive_successor(predecessor, PrimitiveValue::Null, caused_by)
            }
            Value::Bool(b) => {
                Node::primitive_successor(predecessor, PrimitiveValue::Bool(b), caused_by)
            }
            Value::Number(n) => {
                match n.as_i64() {
                    Some(i) => Node::primitive_successor(predecessor, PrimitiveValue::Int(i), caused_by),
                    None => {
                        let f = n.as_f64().expect("Number was neither i64 nor f64");
                        Node::primitive_successor(predecessor, PrimitiveValue::Float(f), caused_by)
                    }
                }
            }
            Value::String(s) => {
                Node::primitive_successor(predecessor, PrimitiveValue::String(s), caused_by)
            }
            // Composites don't have predecessors, so create them as new
            JsonValue::Array(arr) => {
                Node::new_from_json_array(arr, &caused_by)
            }
            JsonValue::Object(obj) => {
                Node::new_from_json_object(obj, &caused_by)
            }
        }
    }

    pub fn successor(&self, value: PrimitiveValue, caused_by: Arc<Event>) -> Node {
        match self {
            Node::Primitive(primitive) => Node::primitive_successor(primitive.clone(), value, caused_by),
            // Composites don't have tracked data, so create as new
            _ => {
                Node::new_primitive(value, caused_by)
            }
        }
    }

    pub fn new_from_json(value: JsonValue, caused_by: Arc<Event>) -> Node {
        match value {
            JsonValue::Null => {
                Node::new_primitive(PrimitiveValue::Null, caused_by)
            }
            JsonValue::Bool(b) => {
                Node::new_primitive(PrimitiveValue::Bool(b), caused_by)
            }
            JsonValue::Number(n) => {
                match n.as_i64() {
                    Some(i) => Node::new_primitive(PrimitiveValue::Int(i), caused_by),
                    None => {
                        let f = n.as_f64().expect("Number was neither i64 nor f64");
                        Node::new_primitive(PrimitiveValue::Float(f), caused_by)
                    }
                }
            }
            JsonValue::String(s) => {
                Node::new_primitive(PrimitiveValue::String(s), caused_by)
            }
            JsonValue::Array(arr) => {
                Node::new_from_json_array(arr, &caused_by)
            }
            JsonValue::Object(obj) => {
                Node::new_from_json_object(obj, &caused_by)
            }
        }
    }

    pub fn new_from_json_object(obj: Map<String, JsonValue>, caused_by: &Arc<Event>) -> Node {
        Node::Object(
            obj.into_iter()
                .map(|(key, val)|
                    (key, Node::new_from_json(val, caused_by.clone())))
                .collect()
        )
    }

    pub fn new_from_json_array(arr: Vec<JsonValue>, caused_by: &Arc<Event>) -> Node {
        Node::Array(
            arr.into_iter()
                .map(|val| Node::new_from_json(val, caused_by.clone()))
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
            ChangeType::New(value) => {
                format!("{}: Add {:#}", self.path, value)
            }
            ChangeType::Remove => {
                format!("{}: Remove {}", self.path, state.node_at(&self.path)?.to_string())
            }
            ChangeType::Set(value) => {
                format!("{}: Change {} with {:#}", self.path, state.node_at(&self.path)?.to_string(), value)
            }
            ChangeType::Overwrite(value) => {
                format!("{}: Overwrite {} with {:#}", self.path, state.node_at(&self.path)?.to_string(), value)
            }
        };

        Ok(str)
    }
}

#[derive(Clone, Debug)]
pub enum ChangeType {
    // For a newly-added value with no history
    New(JsonValue),
    Remove,
    // For changing a value with history
    Set(PrimitiveValue),
    // For overwriting a value and not connecting it to history
    Overwrite(JsonValue),
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
            components: self.components[0..(index + 1)].to_vec(),
        }
    }

    pub fn extend(&self, end: PathComponent) -> Self {
        let mut components = self.components.clone();
        components.push(end);

        Self {
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            components,
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
        let components: Vec<crate::blaseball_state::PathComponent> = vec![$( $x.into(), )*];

        crate::blaseball_state::Path {
            entity_type: $entity_type_expr,
            entity_id: Some($entity_id_expr),
            components
        }
    }};
}

pub(crate) use json_path;
use crate::api::ChroniclerItem;

impl BlaseballState {
    pub fn from_chron_at_time(at_time: &'static str) -> BlaseballState {
        let event = Arc::new(Event::Start);
        // Collect into a vec to make sure all the endpoints start and can begin fetching right away
        let endpoints: Vec<_> = (Box::new(crate::api::chronicler::ENDPOINT_NAMES.into_iter()
            .map(|endpoint_name|
                (endpoint_name, records_from_chron_at_time(endpoint_name, at_time, event.clone()))
            )) as Box<dyn Iterator<Item=(&'static str, Box<dyn Iterator<Item=(Uuid, Node)>>)>>)
            .chain(Box::new(iter::once(
                ("game", schedule_from_chron_at_time(at_time, event.clone()))
            )) as Box<dyn Iterator<Item=(&'static str, Box<dyn Iterator<Item=(Uuid, Node)>>)>>)
            .collect();

        let data = endpoints.into_iter()
            .map(|(endpoint_name, endpoint_iter)|
                {
                    let vec: Vec<_> = endpoint_iter.collect();
                    println!("{} vector had {} elements", endpoint_name, vec.len());
                    let x: EntitySet = vec.into_iter().collect();
                    println!("{} initialized with {} elements", endpoint_name, x.len());
                    (endpoint_name, x)
                }
            )
            .collect();

        BlaseballState {
            predecessor: None,
            from_event: Arc::new(Event::Start),
            data,
        }
    }

    pub fn diff_successor(self: Arc<Self>, caused_by: Arc<Event>, patches: impl IntoIterator<Item=Patch>) -> Result<Arc<BlaseballState>, IngestError> {
        let mut new_data = self.data.clone();

        for patch in patches {
            let context_str = format!("Error applying change {:?}", patch.change);
            apply_change(&mut new_data, patch, caused_by.clone())
                .context(context_str)?;
        }

        Ok(self.successor(caused_by, new_data))
    }

    pub fn successor(self: Arc<Self>, caused_by: Arc<Event>, new_data: BlaseballData) -> Arc<BlaseballState> {
        Arc::new(BlaseballState {
            predecessor: Some(self),
            from_event: caused_by,
            data: new_data,
        })
    }

    pub fn node_at(&self, path: &Path) -> Result<&Node, PathError> {
        let entity_set = self.data.get(path.entity_type)
            .ok_or(PathError::EntityTypeDoesNotExist(path.entity_type))?;
        let entity_id = path.entity_id
            .ok_or(PathError::UnexpectedWildcard(path.entity_type))?;
        let entity = entity_set.get(&entity_id)
            .ok_or(PathError::EntityDoesNotExist(path.entity_type, entity_id))?;

        let mut node = entity;
        for (i, component) in path.components.iter().enumerate() {
            node = match node {
                Node::Object(obj) => {
                    match component {
                        PathComponent::Index(_) => {
                            Err(PathError::UnexpectedType {
                                path: path.slice(i),
                                expected_type: "object",
                                value: node.to_string(),
                            })
                        }
                        PathComponent::Key(key) => {
                            obj.get(key)
                                .ok_or_else(|| PathError::MissingKey(path.slice(i)))
                        }
                    }
                }
                Node::Array(arr) => {
                    match component {
                        PathComponent::Index(idx) => {
                            arr.get(*idx)
                                .ok_or_else(|| PathError::MissingKey(path.slice(i)))
                        }
                        PathComponent::Key(_) => {
                            Err(PathError::UnexpectedType {
                                path: path.slice(i),
                                expected_type: "object",
                                value: node.to_string(),
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
                        value: node.to_string(),
                    })
                }
            }?;
        }

        Ok(node)
    }

    pub fn array_at(&self, path: &Path) -> Result<&im::Vector<Node>, PathError> {
        self.node_at(path)?
            .as_array()
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "array",
                value,
            })
    }

    pub fn int_at(&self, path: &Path) -> Result<i64, PathError> {
        self.node_at(path)?
            .as_int()
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "int",
                value,
            })
    }

    pub fn bool_at(&self, path: &Path) -> Result<bool, PathError> {
        self.node_at(path)?
            .as_bool()
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "int",
                value,
            })
    }

    pub fn string_at(&self, path: &Path) -> Result<String, PathError> {
        self.node_at(path)?
            .as_string()
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "str",
                value,
            })
    }

    pub fn uuid_at(&self, path: &Path) -> Result<Uuid, PathError> {
        self.node_at(path)?
            .as_uuid()
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "uuid",
                value,
            })
    }
}

fn apply_change(data: &mut BlaseballData, change: Patch, caused_by: Arc<Event>) -> Result<(), ApplyChangeError> {
    let entity_set = data.get_mut(change.path.entity_type)
        .ok_or(PathError::EntityTypeDoesNotExist(change.path.entity_type))?;
    let entity_id = change.path.entity_id
        .ok_or(PathError::UnexpectedWildcard(change.path.entity_type))?;

    // Treat the last path component specially because that's how we modify or delete the value
    match change.path.components.clone().split_last() {
        None => {
            // Then we are acting on the entire entity
            apply_change_to_hashmap(entity_set, &entity_id, change, caused_by)
        }
        Some((last, rest)) => {
            let mut node = entity_set.get_mut(&entity_id)
                .ok_or(PathError::EntityDoesNotExist(change.path.entity_type, entity_id))?;

            for (i, component) in rest.iter().enumerate() {
                node = match (node, component) {
                    (Node::Object(obj), PathComponent::Index(_)) => {
                        Err(PathError::UnexpectedType {
                            path: change.path.slice(i),
                            expected_type: "array",
                            value: Node::object_to_string(obj),
                        })
                    }
                    (Node::Object(obj), PathComponent::Key(key)) => {
                        obj.get_mut(key)
                            .ok_or_else(|| PathError::MissingKey(change.path.slice(i)))
                    }
                    (Node::Array(arr), PathComponent::Index(idx)) => {
                        arr.get_mut(*idx)
                            .ok_or_else(|| PathError::MissingKey(change.path.slice(i)))
                    }
                    (Node::Array(arr), PathComponent::Key(_)) => {
                        Err(PathError::UnexpectedType {
                            path: change.path.slice(i),
                            expected_type: "object",
                            value: Node::array_to_string(arr),
                        })
                    }
                    (Node::Primitive(prim), component) => {
                        let expected_type = match component {
                            PathComponent::Index(_) => "array",
                            PathComponent::Key(_) => "object",
                        };

                        Err(PathError::UnexpectedType {
                            path: change.path.slice(i),
                            expected_type,
                            value: Node::primitive_to_string(prim),
                        })
                    }
                }?;
            }

            match node {
                Node::Object(obj) => {
                    match last {
                        PathComponent::Index(_) => {
                            Err(PathError::UnexpectedType {
                                path: change.path,
                                expected_type: "object",
                                value: node.to_string(),
                            }.into())
                        }
                        PathComponent::Key(key) => {
                            apply_change_to_hashmap(obj, key, change, caused_by)
                        }
                    }
                }
                Node::Array(arr) => {
                    match last {
                        PathComponent::Index(idx) => {
                            apply_change_to_vector(arr, *idx, change, caused_by)
                        }
                        PathComponent::Key(_) => {
                            Err(PathError::UnexpectedType {
                                path: change.path,
                                expected_type: "array",
                                value: node.to_string(),
                            }.into())
                        }
                    }
                }
                _ => {
                    let expected_type = match last {
                        PathComponent::Index(_) => "array",
                        PathComponent::Key(_) => "object",
                    };

                    Err(PathError::UnexpectedType {
                        path: change.path,
                        expected_type,
                        value: node.to_string(),
                    }.into())
                }
            }
        }
    }
}

fn apply_change_to_hashmap<T>(container: &mut im::HashMap<T, Node>, key: &T, change: Patch, caused_by: Arc<Event>) -> Result<(), ApplyChangeError>
    where T: Clone + std::hash::Hash + std::cmp::Eq + Display {
    match change.change {
        ChangeType::New(value) => {
            let new_node = apply_change_new(change.path, container.get(key), value, caused_by)?;
            container.insert(key.clone(), new_node);
        }
        ChangeType::Remove => {
            let removed = container.remove(key);
            if removed.is_none() {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
        }
        ChangeType::Set(value) => {
            apply_change_set(change.path, container.get_mut(key), value, caused_by)?;
        }
        ChangeType::Overwrite(value) => {
            let new_node = apply_change_overwrite(change.path, container.get(key), value, caused_by)?;
            container.insert(key.clone(), new_node);
        }
    }

    Ok(())
}

fn apply_change_to_vector(container: &mut im::Vector<Node>, idx: usize, change: Patch, caused_by: Arc<Event>) -> Result<(), ApplyChangeError> {
    match change.change {
        ChangeType::New(value) => {
            let new_node = apply_change_new(change.path, container.get(idx), value, caused_by)?;
            container.insert(idx, new_node);
        }
        ChangeType::Remove => {
            if container.get(idx).is_none() {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
            container.remove(idx);
        }
        ChangeType::Set(value) => {
            apply_change_set(change.path, container.get_mut(idx), value, caused_by)?;
        }
        ChangeType::Overwrite(value) => {
            let new_node = apply_change_overwrite(change.path, container.get(idx), value, caused_by)?;
            container.insert(idx, new_node);
        }
    }

    Ok(())
}


fn apply_change_new(path: Path, current_node: Option<&Node>, new_value: Value, caused_by: Arc<Event>) -> Result<Node, ApplyChangeError> {
    match current_node {
        Some(existing_node) => {
            Err(ApplyChangeError::UnexpectedValue(path, existing_node.to_string()))
        }
        None => { Ok(Node::new_from_json(new_value, caused_by)) }
    }
}

fn apply_change_set(path: Path, current_node: Option<&mut Node>, new_value: PrimitiveValue, caused_by: Arc<Event>) -> Result<(), ApplyChangeError> {
    match current_node {
        Some(existing_node) => {
            *existing_node = existing_node.successor(new_value, caused_by);
            Ok(())
        }
        None => { Err(ApplyChangeError::MissingValue(path)) }
    }
}

fn apply_change_overwrite(path: Path, current_node: Option<&Node>, new_value: Value, caused_by: Arc<Event>) -> Result<Node, ApplyChangeError> {
    match current_node {
        Some(_) => { Ok(Node::new_from_json(new_value, caused_by)) }
        None => { Err(ApplyChangeError::MissingValue(path)) }
    }
}

fn entity_to_hashmap_entry(entity_type: &'static str, item: ChroniclerItem, caused_by: Arc<Event>) -> (Uuid, Node) {
    let obs = Observation {
        entity_type,
        entity_id: item.entity_id,
        observed_at: item.valid_from,
    };

    (obs.entity_id, Node::new_from_json(item.data, caused_by))
}

fn records_from_chron_at_time(entity_type: &'static str, at_time: &'static str, caused_by: Arc<Event>) -> Box<dyn Iterator<Item=(Uuid, Node)>> {
    let iter = crate::api::chronicler::entities(entity_type, at_time)
        .map(move |item| entity_to_hashmap_entry(entity_type, item, caused_by.clone()));

    Box::new(iter)
}


fn schedule_from_chron_at_time(at_time: &'static str, caused_by: Arc<Event>) -> Box<dyn Iterator<Item=(Uuid, Node)>> {
    let iter = crate::api::chronicler::schedule(at_time)
        .map(move |item| entity_to_hashmap_entry("game", item, caused_by.clone()));

    Box::new(iter)
}
