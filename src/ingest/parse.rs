use std::collections::HashSet;
use std::rc::Rc;
use thiserror::Error;
use std::fmt::Write;
use serde_json::Value as JsonValue;
use log::debug;

use crate::blaseball_state::{BlaseballState, Event, KnownValue, PropertyValue, Uuid, Value as StateValue, Value};
use crate::ingest::eventually_schema::{EventType, EventuallyEvent};
