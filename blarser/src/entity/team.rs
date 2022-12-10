use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare, Spurious};
use partial_information_derive::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity, EntityRaw, WrongEntityError};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct TeamState {
    pub redacted: Option<bool>,
    pub nullified: Option<bool>,
    pub scattered: Option<TeamScatteredInfo>,
    #[serde(rename = "imp_motion")] // override the rename_all = "camelCase"
    pub imp_motion: Option<Vec<ImpMotionEntry>>,
    pub perm_mod_sources: Option<HashMap<String, Vec<Uuid>>>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ImpMotionEntry {
    day: i32,
    season: i32,
    // I would like this to be a tuple but I don't want to figure out the macro magic to make that happen
    im_position: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct TeamScatteredInfo {
    full_name: String,
    location: String,
    nickname: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Team {
    pub id: Uuid,
    pub card: Option<i32>,
    pub emoji: String,
    pub level: Option<i32>,
    pub state: Option<TeamState>,
    pub lineup: Vec<Uuid>,
    pub slogan: String,
    pub shadows: Option<Vec<Uuid>>,
    pub bench: Option<Vec<Uuid>>,
    pub bullpen: Option<Vec<Uuid>>,
    pub stadium: Option<Uuid>,
    pub deceased: Option<bool>,
    pub full_name: String,
    pub game_attr: Vec<String>,
    pub league_id: Option<Uuid>,
    pub location: String,
    pub nickname: String,
    pub perm_attr: Vec<String>,
    pub rotation: Vec<Uuid>,
    pub seas_attr: Vec<String>,
    pub week_attr: Vec<String>,
    pub evolution: Option<i32>,
    pub main_color: String,
    pub shame_runs: f32,
    pub shorthand: String,
    pub win_streak: Option<Spurious<i32>>,
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
    pub tournament_wins: Option<i32>,
    pub underchampionships: Option<i32>,

    #[allow(non_snake_case)] pub eDensity: Option<f32>,
    #[allow(non_snake_case)] pub eVelocity: Option<f32>,
    #[allow(non_snake_case)] pub imPosition: Option<f32>,
}

impl Display for Team {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.state.as_ref().and_then(|state| state.scattered.as_ref()).map(|info| &info.full_name) {
            Some(name) => write!(f, "Team: {}", name),
            None => write!(f, "Team: {}", self.full_name),
        }
    }
}

impl EntityRaw for <Team as PartialInformationCompare>::Raw {
    type Entity = Team;

    fn name() -> &'static str { "team" }
    fn id(&self) -> Uuid { self.id }
}

impl Entity for Team {
    fn entity_type(&self) -> &'static str { "team" }
    fn id(&self) -> Uuid { self.id }

    fn description(&self) -> String {
        self.full_name.to_string()
    }
}

impl Team {
    pub fn batter_for_count(&self, count: usize) -> Uuid {
        self.lineup[count % self.lineup.len()]
    }

    pub fn active_pitcher(&self, day: i32) -> Uuid {
        self.rotation[day as usize % self.rotation.len()]
    }
}