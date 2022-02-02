use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;
use crate::api::{EventuallyEvent};
use crate::ingest::sim::{Entity, FeedEventChangeResult};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct SimState {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Sim {
    id: String,
    day: i32,
    attr: Vec<String>,
    menu: String,
    phase: i32,
    rules: Uuid,
    state: SimState,
    league: Uuid,
    season: i32,
    sim_end: DateTime<Utc>,
    era_color: String,
    era_title: String,
    playoffs: Option<()>, // TODO what's the type when it's not null?
    season_id: Uuid,
    sim_start: DateTime<Utc>,
    agitations: i32, // what
    tournament: i32,
    gods_day_date: DateTime<Utc>,
    salutations: i32,
    sub_era_color: String,
    sub_era_title: String,
    terminology: Uuid,
    election_date: DateTime<Utc>,
    endseason_date: DateTime<Utc>,
    midseason_date: DateTime<Utc>,
    next_phase_time: DateTime<Utc>,
    preseason_date: DateTime<Utc>,
    earlseason_date: DateTime<Utc>,
    earlsiesta_date: DateTime<Utc>,
    lateseason_date: DateTime<Utc>,
    latesiesta_date: DateTime<Utc>,
    tournament_round: i32,
    earlpostseason_date: DateTime<Utc>,
    latepostseason_date: DateTime<Utc>,

}

impl Entity for Sim {
    fn apply_event(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        match event.r#type {
            other => {
                panic!("{:?} event does not apply to Sim", other)
            }
        }
    }

    fn could_be(&self, other: &Self) -> bool {
        todo!()
    }
}