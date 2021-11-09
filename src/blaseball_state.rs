use std::collections;
use im;
use std::hash::Hash;
use std::rc::Rc;
use indenter::indented;
use std::fmt::Write;
use serde_json as json;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Uuid(String);

#[derive(Debug)]
pub enum Event {
    Start,
    StructuralUpdate {
        endpoint: &'static str,
    },
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

pub enum ValueDiff<'a> {
    KeysRemoved(Vec<String>),

    KeysAdded(collections::HashMap<String, &'a json::Value>),

    ArraySizeChanged {
        before: usize,
        after: usize,
    },

    ValueChanged {
        before: &'a Value,
        after: &'a json::Value,
    },

    ObjectDiff(collections::HashMap<String, ValueDiff<'a>>),
    ArrayDiff(collections::HashMap<usize, ValueDiff<'a>>),
}

impl ValueDiff<'_> {
    pub(crate) fn is_valid_structural_update(&self) -> bool {
        match self {
            ValueDiff::KeysRemoved(_) => true,
            ValueDiff::KeysAdded(_) => true,
            ValueDiff::ArraySizeChanged { after, .. } => after == &0,
            ValueDiff::ValueChanged { .. } => false,
            ValueDiff::ObjectDiff(children) => {
                children.into_iter().all(|(_, diff)| diff.is_valid_structural_update())
            }
            ValueDiff::ArrayDiff(children) => {
                children.into_iter().all(|(_, diff)| diff.is_valid_structural_update())
            }
        }
    }
}

impl std::fmt::Display for ValueDiff<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ValueDiff::KeysRemoved(keys) => {
                write!(f, "Keys removed: {}", keys.join(", "))
            }
            ValueDiff::KeysAdded(pairs) => {
                write!(f, "Keys added:")?;
                for (key, value) in pairs {
                    write!(f, "\n    - {}: `{:?}`", key, value)?;
                }
                Ok(())
            }
            ValueDiff::ArraySizeChanged { before, after } => {
                write!(f, "Array size changed from {} to {}", before, after)
            }
            ValueDiff::ValueChanged { before, after } => {
                write!(f, "Value changed from `{:?}` to `{:?}`", before, after)
            }
            ValueDiff::ObjectDiff(children) => {
                    for (key, err) in children {
                        write!(f, "\n    - {}: ", key)?;
                        write!(indented(f).with_str("    "), "{}", err)?;
                    }
                    Ok(())
            }
            ValueDiff::ArrayDiff(children) => {
                    for (index, err) in children {
                        write!(f, "\n    - {}: ", index)?;
                        write!(indented(f).with_str("    "), "{}", err)?;
                    }
                    Ok(())
            }
        }
    }
}


impl Uuid {
    pub fn new(s: String) -> Uuid {
        Uuid(s)
    }
}


impl BlaseballState {
    pub fn from_chron_at_time(at_time: &'static str) -> BlaseballState {
        // Start all the endpoints first
        let endpoints: Vec<_> = crate::api::chronicler::ENDPOINT_NAMES.into_iter().map(|endpoint_name|
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
    crate::api::chronicler::entities(entity_type, at_time)
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