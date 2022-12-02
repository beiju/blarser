use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity, EntityRaw, WrongEntityError};
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

impl EntityRaw for <Sim as PartialInformationCompare>::Raw {
    type Entity = Sim;

    fn name() -> &'static str { "sim" }
    fn id(&self) -> Uuid { Uuid::nil() }

    // fn init_events(&self, after_time: DateTime<Utc>) -> Vec<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
    //     if self.phase == 1 && self.earlseason_date > after_time {
    //         vec![(
    //             AnyEvent::EarlseasonStart(EarlseasonStart::new(self.earlseason_date)),
    //             vec![
    //                 ("sim".to_string(), None, serde_json::Value::Null),
    //                 ("game".to_string(), None, serde_json::Value::Null)
    //             ]
    //         )]
    //     } else {
    //         todo!()
    //     }
    // }
}

impl Entity for Sim {
    fn entity_type(self) -> &'static str { "sim" }
    fn id(&self) -> Uuid { Uuid::nil() }
}