use im;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use serde_json as json;
use crate::chronicler;
use crate::chronicler::ENDPOINT_NAMES;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Uuid(String);

#[derive(Debug)]
pub enum Event {
    Start,
    BigDeal {
        feed_event_id: Uuid,
    },
}

#[derive(Debug, Clone)]
pub struct BlaseballState {
    pub predecessor: Option<Rc<BlaseballState>>,
    pub from_event: Rc<Event>,
    pub data: im::HashMap<&'static str, EntitySet>,
}

// The top levels of the state need to be handled directly, because they're separate objects in
// Chron.
pub type EntitySet = im::HashMap<Uuid, Value>;

#[derive(Debug, Clone)]
pub enum Value {
    Object(im::HashMap<String, Value>),
    Array(im::Vector<Value>),
    Value(Rc<TrackedValue>),
}

#[derive(Debug)]
pub struct TrackedValue {
    pub predecessor: Option<Rc<TrackedValue>>,
    pub value: PropertyValue,
}

#[derive(Debug)]
pub enum PropertyValue {
    Known(KnownValue),
    Unknown(UnknownValue),
}

#[derive(Debug)]
pub enum KnownValue {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    Deleted,
}

#[derive(Debug)]
pub enum UnknownValue {
    IntRange {
        lower: i64,
        upper: i64,
    },
    DoubleRange {
        lower: f64,
        upper: f64,
    },
}

impl Uuid {
    pub fn new(s: String) -> Uuid {
        Uuid(s)
    }
}


impl BlaseballState {
    pub fn from_chron_at_time(at_time: &'static str) -> BlaseballState {
        // Start all the endpoints first
        let endpoints: Vec<_> = ENDPOINT_NAMES.into_iter().map(|endpoint_name|
            (endpoint_name, records_from_chron_at_time(endpoint_name, at_time))).collect();

        BlaseballState {
            predecessor: None,
            from_event: Rc::new(Event::Start),
            data: endpoints.into_iter().map(|(endpoint_name, endpoint_iter)|
                (endpoint_name, endpoint_iter.collect())
            ).collect(),
        }
    }
}

fn records_from_chron_at_time(entity_type: &'static str, at_time: &'static str) -> impl Iterator<Item=(Uuid, Value)> {
    chronicler::entities(entity_type, at_time)
        .map(|item| (Uuid(item.entity_id), node_from_json(item.data)))
}

fn node_from_json(value: json::Value) -> Value {
    match value {
        json::Value::Null => root_property(KnownValue::Null),
        json::Value::Bool(b) => root_property(KnownValue::Bool(b)),
        json::Value::Number(n) => match n.as_i64() {
            Some(i) => root_property(KnownValue::Int(i)),
            None => root_property(KnownValue::Double(n.as_f64().unwrap()))
        },
        json::Value::String(s) => root_property(KnownValue::String(s)),
        json::Value::Array(arr) => Value::Array(
            arr.into_iter().map(|item| node_from_json(item)).collect()
        ),
        json::Value::Object(obj) => Value::Object(
            obj.into_iter().map(|(key, item)| (key, node_from_json(item))).collect()
        ),
    }
}

fn root_property(value: KnownValue) -> Value {
    Value::Value(Rc::new(TrackedValue {
        predecessor: None,
        value: PropertyValue::Known(value),
    }))
}