use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::sim::Entity;
use crate::sim::entity::{EarliestEvent, TimedEvent, TimedEventType};

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

impl Display for Sim {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sim")
    }
}

impl Entity for Sim {
    fn name() -> &'static str {
        "sim"
    }
    fn id(&self) -> Uuid { Uuid::nil() }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<TimedEvent> {
        let mut earliest = EarliestEvent::new(after_time);

        earliest.push(TimedEvent {
            time: self.earlseason_date,
            event_type: TimedEventType::EarlseasonStart
        });

        earliest.into_inner()
    }

    fn time_range_for_update(valid_from: DateTime<Utc>, _: &Self::Raw) -> (DateTime<Utc>, DateTime<Utc>) {
        // Sim seems to be timestamped before the fetch? not sure
        (valid_from, valid_from + Duration::minutes(1))
    }
}