use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::entity::AnyEntity;
use crate::events::{Effect, Event, Extrapolated, ord_by_time};

#[derive(Serialize, Deserialize)]
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

    fn effects(&self) -> Vec<Effect> {
        Vec::new()
    }

    fn forward(&self, _: &AnyEntity, _: &Box<dyn Extrapolated>) -> AnyEntity {
        panic!("Cannot re-apply a Start event");
    }

    fn reverse(&self, _: AnyEntity, _: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

ord_by_time!(Start);