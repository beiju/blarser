use std::fmt::{Debug, Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity, EntityRaw, WrongEntityError};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Season {
    #[serde(rename = "__v")]
    pub version: Option<i32>,

    #[serde(alias = "_id")]
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

impl EntityRaw for <Season as PartialInformationCompare>::Raw {
    type Entity = Season;

    fn name() -> &'static str { "season" }
    fn id(&self) -> Uuid { self.id }

    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from - Duration::minutes(1)
    }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from + Duration::minutes(1)
    }
}

impl Into<AnyEntity> for Season {
    fn into(self) -> AnyEntity {
        AnyEntity::Season(self)
    }
}

impl TryFrom<AnyEntity> for Season {
    type Error = WrongEntityError;

    fn try_from(value: AnyEntity) -> Result<Self, Self::Error> {
        match value {
            AnyEntity::Season(value) => { Ok(value) }
            other => Err(WrongEntityError { expected: "season", found: other.name() })
        }
    }
}

impl Entity for Season {
    fn name() -> &'static str { "season" }
    fn id(&self) -> Uuid { self.id }
}