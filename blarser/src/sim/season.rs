use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::sim::Entity;
use crate::sim::entity::TimedEvent;

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

impl Entity for Season {
    fn name() -> &'static str { "season" }
    fn id(&self) -> Uuid { self.id }

    fn next_timed_event(&self, _: DateTime<Utc>) -> Option<TimedEvent> {
        None
    }

    fn time_range_for_update(valid_from: DateTime<Utc>, _: &Self::Raw) -> (DateTime<Utc>, DateTime<Utc>) {
        // It's definitely timestamped after when it's extracted from streamData, but it may also be
        // polled and timestamped before in that case
        (valid_from - Duration::minutes(1), valid_from + Duration::minutes(1))
    }
}
