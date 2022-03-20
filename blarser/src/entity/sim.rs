use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::entity::{Entity, EntityRawTrait, EntityTrait};
use crate::entity::timed_event::{TimedEvent, TimedEventType};

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
    pub menu: String,
    pub rules: Uuid,
    pub state: SimState,
    pub league: Uuid,
    pub season: i32,
    pub sim_end: DateTime<Utc>,
    pub era_color: String,
    pub era_title: String,
    pub playoffs: Option<i32>,
    // TODO what's the type when it's not null?
    pub season_id: Uuid,
    pub sim_start: DateTime<Utc>,
    pub agitations: i32,
    // what
    pub tournament: i32,
    pub gods_day_date: DateTime<Utc>,
    pub salutations: i32,
    pub sub_era_color: String,
    pub sub_era_title: String,
    pub terminology: Uuid,
    pub election_date: DateTime<Utc>,
    pub endseason_date: DateTime<Utc>,
    pub midseason_date: DateTime<Utc>,
    pub next_phase_time: DateTime<Utc>,
    pub preseason_date: DateTime<Utc>,
    pub earlseason_date: DateTime<Utc>,
    pub earlsiesta_date: DateTime<Utc>,
    pub lateseason_date: DateTime<Utc>,
    pub latesiesta_date: DateTime<Utc>,
    pub tournament_round: i32,
    pub earlpostseason_date: DateTime<Utc>,
    pub latepostseason_date: DateTime<Utc>,
}

impl Display for Sim {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sim")
    }
}

impl EntityRawTrait for <Sim as PartialInformationCompare>::Raw {
    fn entity_type(&self) -> &'static str { "sim" }
    fn entity_id(&self) -> Uuid { Uuid::nil() }

    fn init_events(&self, after_time: DateTime<Utc>) -> Vec<TimedEvent> {
        if self.phase == 2 && self.earlseason_date > after_time {
            vec![TimedEvent {
                time: self.earlseason_date,
                event_type: TimedEventType::EarlseasonStart,
            }]
        } else {
            todo!()
        }
    }

    // Sim seems to be timestamped before the fetch? not sure
    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> { valid_from }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> { valid_from + Duration::minutes(1) }

    fn as_entity(self) -> Entity {
        Sim::from_raw(self)
    }
}

impl EntityTrait for Sim {
    fn entity_type(&self) -> &'static str { "sim" }
    fn entity_id(&self) -> Uuid { Uuid::nil() }
}