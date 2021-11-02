use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use serde_json::Value;
use serde_json::value as json;
use crate::chronicler;

struct Uuid(String);

type RecordSet = HashSet<Record>;

struct Event {
    // TODO
}

struct BlaseballState {
    predecessor: Option<Rc<BlaseballState>>,
    from_event: Option<Rc<Event>>,
    sim: String,
    players: RecordSet,
    teams: RecordSet,
}

type PropertySet = HashSet<Property>;

struct Record {
    id: Uuid,
    properties: PropertySet,
}

struct Property {
    name: String,
    predecessor: Option<Rc<Property>>,
    value: PropertyValue,
}

enum PropertyValue {
    Known(KnownValue),
    Unknown(UnknownValue)
}

enum KnownValue {
    Int(i64),
    Double(f64),
    String(String),
    Deleted,
}

enum UnknownValue {
    IntRange {
        lower: i64,
        upper: i64,
    },
    DoubleRange {
        lower: f64,
        upper: f64,
    },
}

impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq<Self> for Record {
    fn eq(&self, other: &Self) -> bool {
        self.id.0.eq(&other.id.0)
    }
}

impl Eq for Record {

}

impl Hash for Property {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state)
    }
}

impl PartialEq<Self> for Property {
    fn eq(&self, other: &Self) -> bool {
        self.name.eq(&other.name)
    }
}

impl Eq for Property {

}

impl Hash for Uuid {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl BlaseballState {
    pub fn from_chron_at_time(sim: String, at_time: &'static str) -> BlaseballState {
        BlaseballState {
            predecessor: None,
            from_event: None,
            players: records_from_chron_at_time(&sim, "player", at_time),
            teams: records_from_chron_at_time(&sim, "team", at_time),
            sim: sim.into(),
        }
    }
}

fn records_from_chron_at_time(_sim: &str, entity_type: &'static str, at_time: &'static str) -> RecordSet {
    chronicler::entities(entity_type, at_time).into_iter()
        .map(|item| Record{
            id: Uuid(item.entity_id),
            properties: properties_from_json(item.data)
        })
        .collect()
}

fn properties_from_json(data: HashMap<String, json::Value>) -> PropertySet {
    data.into_iter()
        .map(|(key, value)| Property{
            name: key,
            predecessor: None,
            value: value_from_json(value)
        })
        .collect()
}

fn value_from_json(value: json::Value) -> PropertyValue {
    match value {
        json::Value::Null => panic!("Unexpected null"),
        json::Value::Bool(_) => panic!("Unexpected bool"),
        json::Value::Number(n) => match n.as_i64() {
            Some(i) => PropertyValue::Known(KnownValue::Int(i)),
            None => PropertyValue::Known(KnownValue::Double(n.as_f64().unwrap()))
        },
        Value::String(s) => PropertyValue::Known(KnownValue::String(s)),
        Value::Array(_) => panic!("Unexpected array"),
        Value::Object(_) => panic!("Unexpected object"),
    }
}