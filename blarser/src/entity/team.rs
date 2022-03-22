use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{Conflict, PartialInformationCompare, Spurious};
use partial_information_derive::PartialInformationCompare;

use crate::entity::{Entity, EntityRaw, EntityRawTrait, EntityTrait};

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
}

impl Display for Team {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.state.as_ref().and_then(|state| state.scattered.as_ref()).map(|info| &info.full_name) {
            Some(name) => write!(f, "Team: {}", name),
            None => write!(f, "Team: {}", self.full_name),
        }
    }
}

impl EntityRawTrait for <Team as PartialInformationCompare>::Raw {
    fn entity_type(&self) -> &'static str { "team" }
    fn entity_id(&self) -> Uuid { self.id }

    // Teams are timestamped before the fetch
    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from
    }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from + Duration::minutes(1)
    }

    fn as_entity(self) -> Entity {
        Entity::Team(Team::from_raw(self))
    }
    fn to_json(self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("Error serializing TeamRaw object")
    }
}

impl EntityTrait for Team {
    fn entity_type(&self) -> &'static str { "team" }
    fn entity_id(&self) -> Uuid { self.id }

    fn observe(&mut self, raw: &EntityRaw) -> Vec<Conflict> {
        if let EntityRaw::Team(raw) = raw {
            PartialInformationCompare::observe(self, raw)
        } else {
            panic!("Tried to observe {} with an observation from {}",
                   self.entity_type(), raw.entity_type());
        }
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