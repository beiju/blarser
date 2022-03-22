use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::events::Event;

#[derive(Serialize, Deserialize)]
pub struct Start {
    time: DateTime<Utc>,
}

impl Start {
    pub fn new(time: DateTime<Utc>) -> Self {
        Start { time }
    }
}

impl Event for Start {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, _: AnyEntity, _: serde_json::Value) -> AnyEntity {
        panic!("Cannot re-apply a Start event");
    }

    fn reverse(&self, _: AnyEntity, _: serde_json::Value) -> AnyEntity {
        todo!()
    }
}