use chrono::{DateTime, Timelike, Utc};
use serde::Deserialize;
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::sim::{Entity, FeedEventChangeResult};
use crate::sim::entity::EarliestEvent;
use crate::state::{GenericEvent, GenericEventType, StateInterface};

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SimState {}

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
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

    fn next_timed_event(&self, from_time: DateTime<Utc>, to_time: DateTime<Utc>, _state: &StateInterface) -> Option<GenericEvent> {
        let mut earliest = EarliestEvent::new();

        earliest.push_opt(self.get_earlseason_start(from_time, to_time));
        earliest.push_opt(self.get_day_advance(from_time, to_time));

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

impl Sim {
    pub fn get_earlseason_start(&self, from_time: DateTime<Utc>, to_time: DateTime<Utc>) -> Option<GenericEvent> {
        if from_time < self.earlseason_date && self.earlseason_date < to_time  {
            Some(GenericEvent {
                time: self.earlseason_date,
                event_type: GenericEventType::EarlseasonStart,
            })
        } else {
            None
        }
    }

    pub fn get_day_advance(&self, from_time: DateTime<Utc>, to_time: DateTime<Utc>) -> Option<GenericEvent> {
        // I'm sure I'm going to need to add more phases where days advance
        if self.phase == 2 && from_time.hour() != to_time.hour() {
            Some(GenericEvent {
                time: to_time
                    .with_minute(0).unwrap()
                    .with_second(0).unwrap()
                    .with_nanosecond(0).unwrap(),
                event_type: GenericEventType::DayAdvance,
            })
        }
        else {
            None
        }
    }
}