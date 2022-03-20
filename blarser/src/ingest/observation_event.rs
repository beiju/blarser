use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub struct ChronObservationEvent {
    pub entity_type: String,
    pub entity_id: Uuid,
    pub perceived_at: DateTime<Utc>,
    pub applied_at: DateTime<Utc>,
}

impl ChronObservationEvent {
    pub fn description(&self) -> String {
        format!("Chron observation for {} {} percieved at {}, applied at {}",
                self.entity_type, self.entity_id, self.perceived_at, self.applied_at)
    }
}