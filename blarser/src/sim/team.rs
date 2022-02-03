use serde::Deserialize;
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::sim::{Entity, FeedEventChangeResult};
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct TeamState {}

#[derive(Clone, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Team {
    pub id: Uuid,
    pub card: i32,
    pub emoji: String,
    pub level: i32,
    pub state: TeamState,
    pub lineup: Vec<Uuid>,
    pub slogan: String,
    pub shadows: Vec<Uuid>,
    pub stadium: Option<Uuid>,
    pub deceased: bool,
    pub full_name: String,
    pub game_attr: Vec<String>,
    pub league_id: Option<Uuid>,
    pub location: String,
    pub nickname: String,
    pub perm_attr: Vec<String>,
    pub rotation: Vec<Uuid>,
    pub seas_attr: Vec<String>,
    pub week_attr: Vec<String>,
    pub evolution: i32,
    pub main_color: String,
    pub shame_runs: i32,
    pub shorthand: String,
    pub win_streak: i32,
    pub division_id: Option<Uuid>,
    pub team_spirit: i32,
    pub subleague_id: Option<Uuid>,
    pub total_shames: i32,
    pub rotation_slot: i32,
    pub season_shames: i32,
    pub championships: i32,
    pub total_shamings: i32,
    pub season_shamings: i32,
    pub secondary_color: String,
    pub tournament_wins: i32,
    pub underchampionships: i32,
}

impl Entity for Team {
    fn name() -> &'static str {
        "team"
    }

    fn apply_event(&mut self, event: &GenericEvent, _state: &StateInterface) -> FeedEventChangeResult {
        match &event.event_type {
            GenericEventType::FeedEvent(event) => self.apply_feed_event(event),
            other => {
                panic!("{:?} event does not apply to Team", other)
            }
        }
    }
}

impl Team {
    fn apply_feed_event(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        match event.r#type {
            other => {
                panic!("{:?} event does not apply to Team", other)
            }
        }
    }

    pub fn batter_for_count(&self, count: usize) -> Uuid {
        self.lineup[count % self.lineup.len()]
    }
}