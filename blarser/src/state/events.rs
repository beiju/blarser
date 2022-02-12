use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::api::EventuallyEvent;
use crate::StateInterface;

pub trait IngestEvent {
    fn apply(&self, state: &mut StateInterface);
}