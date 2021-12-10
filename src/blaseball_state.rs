use std::fmt::{Debug, Display, Formatter};
use std::iter;
use std::str::FromStr;
use im;
use uuid::Uuid;
use std::sync::Arc;
use tokio::sync::{RwLock};
use rocket::futures::stream::{self, StreamExt};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value as JsonValue, Value};
use thiserror::Error;
use crate::ingest::{IngestError};
use async_recursion::async_recursion;
use im::{HashMap, Vector};

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
pub type EntitySet = im::HashMap<Uuid, Node>;

pub type SharedPrimitiveNode = Arc<RwLock<PrimitiveNode>>;

#[derive(Debug, Clone)]
pub enum Node {
    Object(im::HashMap<String, Node>),
    Array(im::Vector<Node>),
    Primitive(SharedPrimitiveNode),
}

#[derive(Debug)]
pub struct PrimitiveNode {
    pub predecessor: Option<SharedPrimitiveNode>,
    pub caused_by: Arc<Event>,
    pub observed_by: Option<Observation>,
    pub value: PrimitiveValue,
}

#[derive(Debug, Clone)]
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
                f.write_str(s)
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

impl From<i32> for PrimitiveValue {
    fn from(i: i32) -> Self {
        PrimitiveValue::Int(i.into())
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

    pub fn as_bool(&self) -> Option<&bool> {
        match self {
            PrimitiveValue::Bool(b) => { Some(b) }
            _ => { None }
        }
    }

    pub fn as_int(&self) -> Option<&i64> {
        match self {
            PrimitiveValue::Int(i) => { Some(i) }
            _ => { None }
        }
    }

    pub fn as_float(&self) -> Option<&f64> {
        match self {
            PrimitiveValue::Float(f) => { Some(f) }
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

    #[error("Path error at {path}: Expected {expected_type} but found {value}")]
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

    #[error("Cannot increment value {1} at {0}")]
    CannotIncrement(Path, String),

    #[error(transparent)]
    PathError {
        #[from]
        from: PathError
    },

}

impl Node {
    #[async_recursion]
    pub async fn to_string(&self) -> String {
        match self {
            Node::Object(obj) => {
                Self::object_to_string(obj).await
            }
            Node::Array(arr) => {
                Self::array_to_string(arr).await
            }
            Node::Primitive(primitive) => {
                Self::primitive_to_string(primitive).await
            }
        }
    }

    pub async fn as_array(&self) -> Result<&im::Vector<Node>, String> {
        match self {
            Node::Array(arr) => { Ok(arr) }
            _ => { Err(self.to_string().await) }
        }
    }

    pub async fn as_int(&self) -> Result<i64, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().await;
                primitive.value.as_int().cloned()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string().await) }
        }
    }

    pub async fn as_bool(&self) -> Result<bool, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().await;
                primitive.value.as_bool().cloned()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string().await) }
        }
    }

    pub async fn as_string(&self) -> Result<String, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().await;
                primitive.value.as_str().map(|s| s.to_string())
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string().await) }
        }
    }

    pub async fn as_uuid(&self) -> Result<Uuid, String> {
        match self {
            Node::Primitive(primitive_shared) => {
                let primitive = primitive_shared.read().await;
                primitive.value.as_uuid()
                    .ok_or_else(|| primitive.value.to_string())
            }
            _ => { Err(self.to_string().await) }
        }
    }


    pub async fn primitive_to_string(primitive: &SharedPrimitiveNode) -> String {
        let lock = primitive.read().await;
        format!("{}", lock.value)
    }

    pub async fn array_to_string(arr: &Vector<Node>) -> String {
        let inner = stream::iter(arr)
            .then(|node| node.to_string())
            .collect::<Vec<_>>()
            .await
            .join(", ");

        format!("[{}]", inner)
    }

    pub async fn object_to_string(obj: &HashMap<String, Node>) -> String {
        if obj.is_empty() {
            // Shortcut for two braces without spaces
            return "{}".to_string();
        }

        let inner = stream::iter(obj)
            .then(|(key, node)| async move {
                format!("\"{}\": {}", key, node.to_string().await)
            })
            .collect::<Vec<_>>()
            .await
            .join(", ");

        format!("{{ {} }}", inner)
    }

    pub fn new_primitive(value: PrimitiveValue, caused_by: Arc<Event>) -> Node {
        let observed_by = match &*caused_by {
            Event::ImplicitChange(observation) => Some(observation.clone()),
            _ => None
        };

        return Node::Primitive(Arc::new(RwLock::new(PrimitiveNode {
            predecessor: None,
            caused_by,
            observed_by,
            value,
        })));
    }

    pub fn primitive_successor(predecessor: Arc<RwLock<PrimitiveNode>>, value: PrimitiveValue, caused_by: Arc<Event>) -> Node {
        let observed_by = match &*caused_by {
            Event::ImplicitChange(observation) => Some(observation.clone()),
            _ => None
        };

        Node::Primitive(Arc::new(RwLock::new(PrimitiveNode {
            predecessor: Some(predecessor),
            caused_by,
            observed_by,
            value,
        })))
    }

    pub fn successor_from_primitive(predecessor: Arc<RwLock<PrimitiveNode>>, value: JsonValue, caused_by: Arc<Event>) -> Node {
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

    pub fn successor(&self, value: PrimitiveValue, caused_by: Arc<Event>) -> Result<Node, ApplyChangeError> {
        let result = match self {
            Node::Primitive(primitive) => Node::primitive_successor(primitive.clone(), value, caused_by),
            // Composites don't have tracked data, so create as new
            _ => {
                Node::new_primitive(value, caused_by)
            }
        };

        Ok(result)
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
                    (key.clone(), Node::new_from_json(val, caused_by.clone())))
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
    pub async fn description(&self, state: &BlaseballState) -> Result<String, PathError> {
        let str = match &self.change {
            ChangeType::New(value) => {
                format!("{}: Add value {:#}", self.path, value)
            }
            ChangeType::Remove => {
                format!("{}: Remove value {}", self.path, state.node_at(&self.path).await?.to_string().await)
            }
            ChangeType::Set(value) => {
                format!("{}: Replace primitive {} with primitive {:#}", self.path, state.node_at(&self.path).await?.to_string().await, value)
            }
            ChangeType::ReplaceWithComposite(value) => {
                format!("{}: Replace primitive {} with composite {:#}", self.path, state.node_at(&self.path).await?.to_string().await, value)
            }
            ChangeType::Increment => {
                format!("{}: Increment {}", self.path, state.node_at(&self.path).await?.to_string().await)
            }
        };

        Ok(str)
    }
}

#[derive(Clone)]
pub enum ChangeType {
    New(JsonValue),
    Remove,
    Set(PrimitiveValue),
    ReplaceWithComposite(JsonValue),
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
use crate::api::ChroniclerItem;

impl BlaseballState {
    pub fn from_chron_at_time(at_time: &'static str) -> BlaseballState {
        let event = Arc::new(Event::Start);
        // Start all the endpoints first
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

    pub async fn successor(self: Arc<Self>, caused_by: Arc<Event>, patches: impl IntoIterator<Item=Patch>) -> Result<Arc<BlaseballState>, IngestError> {
        let mut new_data = self.data.clone();

        for patch in patches {
            apply_change(&mut new_data, patch, caused_by.clone()).await?;
        }

        Ok(Arc::new(BlaseballState {
            predecessor: Some(self),
            from_event: caused_by,
            data: new_data,
        }))
    }

    pub async fn node_at(&self, path: &Path) -> Result<&Node, PathError> {
        let entity_set = self.data.get(path.entity_type)
            .ok_or_else(|| PathError::EntityTypeDoesNotExist(path.entity_type))?;
        let entity_id = path.entity_id
            .ok_or_else(|| PathError::UnexpectedWildcard(path.entity_type))?;
        let entity = entity_set.get(&entity_id)
            .ok_or_else(|| PathError::EntityDoesNotExist(path.entity_type, entity_id))?;

        let mut node = entity;
        for (i, component) in path.components.iter().enumerate() {
            node = match node {
                Node::Object(obj) => {
                    match component {
                        PathComponent::Index(_) => {
                            Err(PathError::UnexpectedType {
                                path: path.slice(i),
                                expected_type: "object",
                                value: node.to_string().await,
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
                                value: node.to_string().await,
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
                        value: node.to_string().await,
                    })
                }
            }?;
        }

        Ok(node)
    }

    pub async fn array_at(&self, path: &Path) -> Result<&im::Vector<Node>, PathError> {
        self.node_at(path).await?
            .as_array().await
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "array",
                value,
            })
    }

    pub async fn int_at(&self, path: &Path) -> Result<i64, PathError> {
        self.node_at(path).await?
            .as_int().await
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "int",
                value,
            })
    }

    pub async fn bool_at(&self, path: &Path) -> Result<bool, PathError> {
        self.node_at(path).await?
            .as_bool().await
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "int",
                value,
            })
    }

    pub async fn string_at(&self, path: &Path) -> Result<String, PathError> {
        self.node_at(path).await?
            .as_string().await
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "str",
                value,
            })
    }

    pub async fn uuid_at(&self, path: &Path) -> Result<Uuid, PathError> {
        self.node_at(path).await?
            .as_uuid().await
            .map_err(|value| PathError::UnexpectedType {
                path: path.clone(),
                expected_type: "str",
                value,
            })
    }
}

async fn apply_change(data: &mut BlaseballData, change: Patch, caused_by: Arc<Event>) -> Result<(), ApplyChangeError> {
    let entity_set = data.get_mut(change.path.entity_type)
        .ok_or_else(|| PathError::EntityTypeDoesNotExist(change.path.entity_type))?;
    let entity_id = change.path.entity_id
        .ok_or_else(|| PathError::UnexpectedWildcard(change.path.entity_type))?;

    // Treat the last path component specially because that's how we modify or delete the value
    match change.path.components.clone().split_last() {
        None => {
            // Then we are acting on the entire entity
            apply_change_to_hashmap(entity_set, &entity_id, change, caused_by).await
        }
        Some((last, rest)) => {
            let mut node = entity_set.get_mut(&entity_id)
                .ok_or_else(|| PathError::EntityDoesNotExist(change.path.entity_type, entity_id))?;

            for (i, component) in rest.iter().enumerate() {
                node = match (node, component) {
                    (Node::Object(obj), PathComponent::Index(_)) => {
                        Err(PathError::UnexpectedType {
                            path: change.path.slice(i),
                            expected_type: "array",
                            value: Node::object_to_string(obj).await,
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
                            value: Node::array_to_string(arr).await,
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
                            value: Node::primitive_to_string(prim).await,
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
                                value: node.to_string().await,
                            }.into())
                        }
                        PathComponent::Key(key) => {
                            apply_change_to_hashmap(obj, key, change, caused_by).await
                        }
                    }
                }
                Node::Array(arr) => {
                    match last {
                        PathComponent::Index(idx) => {
                            apply_change_to_vector(arr, *idx, change, caused_by).await
                        }
                        PathComponent::Key(_) => {
                            Err(PathError::UnexpectedType {
                                path: change.path,
                                expected_type: "array",
                                value: node.to_string().await,
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
                        value: node.to_string().await,
                    }.into())
                }
            }
        }
    }
}

async fn apply_change_to_hashmap<T: Clone + std::hash::Hash + std::cmp::Eq>(container: &mut im::HashMap<T, Node>, key: &T, change: Patch, caused_by: Arc<Event>) -> Result<(), ApplyChangeError> {
    match change.change {
        ChangeType::New(value) => {
            if let Some(value) = container.get(key) {
                return Err(ApplyChangeError::UnexpectedValue(change.path, value.to_string().await));
            }
            container.insert(key.clone(), Node::new_from_json(
                value,
                caused_by,
            ));
        }
        ChangeType::Remove => {
            let removed = container.remove(key);
            if let None = removed {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
        }
        ChangeType::Set(value) => {
            if let Some(node) = container.get(key) {
                let new_node = node.successor(value, caused_by)?;
                container.insert(key.clone(), new_node);
            } else {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
        }
        ChangeType::ReplaceWithComposite(value) => {
            if let Some(_) = container.get(key) {
                let new_node = Node::new_from_json(value, caused_by);
                container.insert(key.clone(), new_node);
            } else {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
        }
        ChangeType::Increment => {
            let new_node = match container.get(key) {
                None => { Err(ApplyChangeError::MissingValue(change.path)) }
                Some(Node::Primitive(primitive)) => {
                    let node = primitive.read().await;
                    let new_node = match &node.value {
                        PrimitiveValue::Int(i) => Node::primitive_successor(
                            primitive.clone(),
                            PrimitiveValue::Int(i + 1),
                            caused_by,
                        ),
                        PrimitiveValue::IntRange(upper, lower) => Node::primitive_successor(
                            primitive.clone(),
                            PrimitiveValue::IntRange(upper + 1, lower + 1),
                            caused_by,
                        ),
                        value => {
                            return Err(ApplyChangeError::CannotIncrement(change.path, value.to_string()));
                        }
                    };
                    Ok(new_node)
                }
                Some(node) => {
                    Err(ApplyChangeError::CannotIncrement(change.path, node.to_string().await))
                }
            }?;
            container.insert(key.clone(), new_node);
        }
    }

    Ok(())
}

async fn apply_change_to_vector(container: &mut im::Vector<Node>, idx: usize, change: Patch, caused_by: Arc<Event>) -> Result<(), ApplyChangeError> {
    match change.change {
        ChangeType::New(value) => {
            if let Some(node) = container.get(idx) {
                return Err(ApplyChangeError::UnexpectedValue(change.path, node.to_string().await));
            }
            container.insert(idx, Node::new_from_json(value, caused_by));
        }
        ChangeType::Remove => {
            if let None = container.get(idx) {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
            container.remove(idx);
        }
        ChangeType::Set(value) => {
            if let Some(node) = container.get(idx) {
                let new_node = node.successor(value, caused_by)?;
                container.insert(idx, new_node);
            } else {
                return Err(ApplyChangeError::MissingValue(change.path));
            }
        }
        _ => { todo!() }
    }

    Ok(())
}

fn entity_to_hashmap_entry(entity_type: &'static str, item: ChroniclerItem, caused_by: Arc<Event>) -> (Uuid, Node) {
    let obs = Observation {
        entity_type,
        entity_id: item.entity_id,
        observed_at: item.valid_from,
    };

    (obs.entity_id.clone(), Node::new_from_json(item.data, caused_by))
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
