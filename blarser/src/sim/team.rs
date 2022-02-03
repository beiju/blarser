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
    id: Uuid,
    card: i32,
    emoji: String,
    level: i32,
    state: TeamState,
    lineup: Vec<Uuid>,
    slogan: String,
    shadows: Vec<Uuid>,
    stadium: Option<Uuid>,
    deceased: bool,
    full_name: String,
    game_attr: Vec<String>,
    league_id: Option<Uuid>,
    location: String,
    nickname: String,
    perm_attr: Vec<String>,
    rotation: Vec<Uuid>,
    seas_attr: Vec<String>,
    week_attr: Vec<String>,
    evolution: i32,
    main_color: String,
    shame_runs: i32,
    shorthand: String,
    win_streak: i32,
    division_id: Option<Uuid>,
    team_spirit: i32,
    subleague_id: Option<Uuid>,
    total_shames: i32,
    rotation_slot: i32,
    season_shames: i32,
    championships: i32,
    total_shamings: i32,
    season_shamings: i32,
    secondary_color: String,
    tournament_wins: i32,
    underchampionships: i32,
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