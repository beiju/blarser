use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::sim::{Entity, FeedEventChangeResult};
use crate::sim::entity::Lowest;
use crate::state::{GenericEvent, GenericEventType, StateInterface};

#[derive(Clone, Debug, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SimState {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialInformationCompare)]
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
    pub playoffs: Option<i32>, // TODO what's the type when it's not null?
    pub season_id: Uuid,
    pub sim_start: DateTime<Utc>,
    pub agitations: i32, // what
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

impl Entity for Sim {
    fn name() -> &'static str {
        "sim"
    }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut earliest = Lowest::new(after_time);

        earliest.push(self.earlseason_date);

        earliest.into_inner()
    }

    fn apply_event(&mut self, event: &GenericEvent, _state: &StateInterface) -> FeedEventChangeResult {
        match &event.event_type {
            GenericEventType::EarlseasonStart => {
                if self.phase == 1 {
                    self.phase = 2;
                    self.next_phase_time = self.earlseason_date;
                    FeedEventChangeResult::Ok
                } else {
                    panic!("Tried to apply EarlseasonStart event while not in Preseason phase")
                }
            }
            GenericEventType::DayAdvance => {
                self.day += 1;
                FeedEventChangeResult::Ok
            }
            other => {
                panic!("{:?} event does not apply to Sim", other)
            }
        }
    }
}