use std::collections::HashMap;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::sim::Entity;
use crate::sim::entity::TimedEvent;
use crate::state::StateInterface;

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
    pub win_streak: Option<i32>,
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

impl Entity for Team {
    fn name() -> &'static str { "team" }
    fn id(&self) -> Uuid { self.id }

    fn next_timed_event(&self, _: DateTime<Utc>) -> Option<TimedEvent> {
        None
    }

    // fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult {
    //     match &event.event_type {
    //         GenericEventType::FeedEvent(event) => self.apply_feed_event(event, state),
    //         other => {
    //             panic!("{:?} event does not apply to Team", other)
    //         }
    //     }
    // }
}

impl Team {
    // fn apply_feed_event(&mut self, event: &EventuallyEvent, _state: &StateInterface) -> FeedEventChangeResult {
    //     match event.r#type {
    //         EventType::GameEnd => {
    //             assert!(event.team_tags.contains(&self.id),
    //                     "Tried to apply GameEnd event to the wrong team");
    //             let winner_id: Uuid = serde_json::from_value(
    //                 event.metadata.other.get("winner")
    //                     .expect("GameEnd event must have a winner in the metadata")
    //                     .clone())
    //                 .expect("Winner property of GameEnd event must be a uuid");
    //
    //             if self.id == winner_id {
    //                 self.win_streak.as_mut()
    //                     .expect("GameEnd currently expects Team.win_streak to exist")
    //                     .add_cached(1, event.created + Duration::minutes(5));
    //             } else {
    //                 self.win_streak.as_mut()
    //                     .expect("GameEnd currently expects Team.win_streak to exist")
    //                     .add_cached(-1, event.created + Duration::minutes(5));
    //             };
    //
    //             FeedEventChangeResult::Ok
    //         }
    //         EventType::PitcherChange => {
    //             // TODO: Fill in actual changes, or delete this if it turns out they don't apply
    //             FeedEventChangeResult::DidNotApply
    //         }
    //         other => {
    //             panic!("{:?} event does not apply to Team", other)
    //         }
    //     }
    // }
    //
    // pub fn batter_for_count(&self, count: usize) -> Uuid {
    //     self.lineup[count % self.lineup.len()]
    // }
    //
    // pub fn active_pitcher(&self, day: i32) -> Uuid {
    //     self.rotation[day as usize % self.rotation.len()]
    // }
}