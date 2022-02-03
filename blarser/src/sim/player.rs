use serde::Deserialize;
use uuid::Uuid;
use partial_information::{Ranged, PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::sim::{Entity, FeedEventChangeResult};
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
pub struct Item {}

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
pub struct PlayerState {}

#[derive(Clone, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Player {
    pub id: Uuid,
    pub name: String,
    pub ritual: String,
    pub fate: i32,
    pub soul: i32,
    pub blood: i32,
    pub coffee: i32,
    pub peanut_allergy: bool,

    pub league_team_id: Uuid,
    pub tournament_team_id: Option<Uuid>,
    pub deceased: bool,
    pub evolution: i32,
    pub items: Vec<Item>,
    pub state: PlayerState,
    pub hit_streak: i32,
    pub consecutive_hits: i32,

    pub game_attr: Vec<String>,
    pub week_attr: Vec<String>,
    pub seas_attr: Vec<String>,
    pub item_attr: Vec<String>,
    pub perm_attr: Vec<String>,

    pub buoyancy: Ranged<f32>,
    pub cinnamon: Ranged<f32>,
    pub coldness: Ranged<f32>,
    pub chasiness: Ranged<f32>,
    pub divinity: Ranged<f32>,
    pub martyrdom: Ranged<f32>,
    pub base_thirst: Ranged<f32>,
    pub indulgence: Ranged<f32>,
    pub musclitude: Ranged<f32>,
    pub tragicness: Ranged<f32>,
    pub omniscience: Ranged<f32>,
    pub patheticism: Ranged<f32>,
    pub suppression: Ranged<f32>,
    pub continuation: Ranged<f32>,
    pub ruthlessness: Ranged<f32>,
    pub watchfulness: Ranged<f32>,
    pub laserlikeness: Ranged<f32>,
    pub overpowerment: Ranged<f32>,
    pub tenaciousness: Ranged<f32>,
    pub thwackability: Ranged<f32>,
    pub anticapitalism: Ranged<f32>,
    pub ground_friction: Ranged<f32>,
    pub pressurization: Ranged<f32>,
    pub unthwackability: Ranged<f32>,
    pub shakespearianism: Ranged<f32>,
    pub moxie: Ranged<f32>,
    pub total_fingers: i32,

    pub defense_rating: f32,
    pub hitting_rating: f32,
    pub pitching_rating: f32,
    pub baserunning_rating: f32,
}

impl Entity for Player {
    fn name() -> &'static str {
        "player"
    }

    fn apply_event(&mut self, event: &GenericEvent, _state: &StateInterface) -> FeedEventChangeResult {
        match &event.event_type {
            GenericEventType::FeedEvent(event) => self.apply_feed_event(event),
            other => {
                panic!("{:?} event does not apply to Player", other)
            }
        }
    }
}

impl Player {
    fn apply_feed_event(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        match event.r#type {
            EventType::FlyOut => {
                self.fielding_out(event, "flyout")
            }
            EventType::GroundOut => {
                self.fielding_out(event, "ground out")
            }
            EventType::Snowflakes => {
                // TODO Ugh why does the most complicated one have to come up first
                // This does apply sometimes and I need to figure that out
                FeedEventChangeResult::DidNotApply
            }
            other => {
                panic!("{:?} event does not apply to Player", other)
            }
        }
    }

    fn fielding_out(&mut self, event: &EventuallyEvent, out_str: &'static str) -> FeedEventChangeResult {
        // TODO Parse flyout description for more robust check
        if event.description.starts_with(&format!("{} hit a {} to ", self.name, out_str)) {
            self.consecutive_hits = 0;
            FeedEventChangeResult::Ok
        } else {
            FeedEventChangeResult::DidNotApply
        }
    }
}