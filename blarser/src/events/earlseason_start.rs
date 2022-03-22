use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::events::Event;

#[derive(Serialize, Deserialize)]
pub struct EarlseasonStart {
    time: DateTime<Utc>,
}

impl EarlseasonStart {
    pub fn new(time: DateTime<Utc>) -> Self {
        EarlseasonStart { time }
    }
}

impl Event for EarlseasonStart {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, _: AnyEntity, _: serde_json::Value) -> AnyEntity {
        todo!()
    }

    fn reverse(&self, _: AnyEntity, _: serde_json::Value) -> AnyEntity {
        todo!()
    }
}