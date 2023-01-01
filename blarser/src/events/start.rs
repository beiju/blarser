use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::events::{AnyEffect, Event, ord_by_time};
use crate::ingest::StateGraph;

#[derive(Debug, Serialize, Deserialize)]
pub struct Start {
    time: DateTime<Utc>,
}

impl Start {
    pub fn new(time: DateTime<Utc>) -> Self {
        Start { time }
    }
}

impl Display for Start {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Start ingest at {}", self.time)
    }
}

impl Event for Start {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn into_effects(self, _: &StateGraph) -> Vec<AnyEffect> {
        Vec::new()
    }
}

ord_by_time!(Start);