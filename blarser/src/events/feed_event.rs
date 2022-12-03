use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use diesel::QueryResult;
use fed::FedEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::{AnyEntity, Entity};
use crate::events::{AnyEvent, Event, ord_by_time};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct FeedEvent(FedEvent);

impl FeedEvent {
    pub fn from_fed(event: FedEvent) -> FeedEvent {
        FeedEvent(event)
    }
    pub fn any_from_fed(event: FedEvent) -> AnyEvent {
        AnyEvent::FeedEvent(FeedEvent(event))
    }
}

impl Event for FeedEvent {
    fn time(&self) -> DateTime<Utc> {
        self.0.created
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        todo!()
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

impl Display for FeedEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} at {}", self.0.data.as_ref(), self.0.created)
    }
}

ord_by_time!(FeedEvent);