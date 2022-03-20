use std::fmt::{Debug, Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::entity::{Entity, EntityRawTrait, EntityTrait, Standings};
use crate::entity::timed_event::TimedEvent;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Season {
    #[serde(rename="__v")]
    pub version: Option<i32>,

    #[serde(alias="_id")]
    pub id: Uuid,

    pub rules: Uuid,
    pub stats: Uuid,
    pub league: Uuid,
    pub schedule: Option<Uuid>,
    pub standings: Uuid,
    pub terminology: Uuid,
    pub season_number: i32,
    pub total_days_in_season: Option<i32>,
}

impl Display for Season {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Season")
    }
}

impl EntityRawTrait for <Season as PartialInformationCompare>::Raw {
    fn entity_type(&self) -> &'static str { "season" }
    fn entity_id(&self) -> Uuid { self.id }

    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from - Duration::minutes(1)
    }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from + Duration::minutes(1)
    }

    fn as_entity(self) -> Entity {
        Entity::Season(Season::from_raw(self))
    }
}

impl EntityTrait for Season {
    fn entity_type(&self) -> &'static str { "season" }
    fn entity_id(&self) -> Uuid { self.id }
}