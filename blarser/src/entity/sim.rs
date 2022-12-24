use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare, DatetimeWithResettingMs};
use partial_information_derive::PartialInformationCompare;

use crate::entity::{Entity, EntityRaw};
use crate::state::EntityType;
// use crate::events::{AnyEvent, EarlseasonStart};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SimState {}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Sim {
    pub phase: i32,
    pub id: String,
    pub day: i32,
    pub attr: Vec<String>,
    pub menu: Option<String>,
    pub rules: Uuid,
    pub state: Option<SimState>,
    pub league: Uuid,
    pub season: i32,
    pub sim_end: Option<DateTime<Utc>>,
    pub era_color: String,
    pub era_title: String,
    pub playoffs: Option<Uuid>,
    pub season_id: Option<Uuid>,
    pub sim_start: Option<DateTime<Utc>>,
    pub agitations: i32, // what
    pub tournament: i32,
    pub gods_day_date: DatetimeWithResettingMs,
    pub salutations: i32,
    pub sub_era_color: String,
    pub sub_era_title: String,
    pub terminology: Uuid,
    pub election_date: DateTime<Utc>,
    pub endseason_date: DateTime<Utc>,
    pub midseason_date: DateTime<Utc>,
    pub next_phase_time: DatetimeWithResettingMs,
    pub preseason_date: DateTime<Utc>,
    pub earlseason_date: DateTime<Utc>,
    pub earlsiesta_date: DateTime<Utc>,
    pub lateseason_date: DateTime<Utc>,
    pub latesiesta_date: DateTime<Utc>,
    pub tournament_round: i32,
    pub play_off_round: Option<i32>,
    pub earlpostseason_date: DateTime<Utc>,
    pub latepostseason_date: DateTime<Utc>,
}

impl Display for Sim {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sim")
    }
}

impl EntityRaw for <Sim as PartialInformationCompare>::Raw {
    type Entity = Sim;

    fn name() -> &'static str { "sim" }
    fn id(&self) -> Uuid { Uuid::nil() }
}

impl Entity for Sim {
    fn entity_type(&self) -> EntityType { EntityType::Sim }
    fn id(&self) -> Uuid { Uuid::nil() }

    fn description(&self) -> String {
        "Sim".to_string()
    }
}