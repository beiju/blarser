use serde::Deserialize;
use uuid::Uuid;
use partial_information::{Ranged, PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::ingest::sim::{Entity, FeedEventChangeResult};

#[derive(Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
pub struct Item {

}

#[derive(Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
pub struct PlayerState {

}

#[derive(Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Player {
    id: Uuid,
    name: String,
    ritual: String,
    fate: i32,
    soul: i32,
    blood: i32,
    coffee: i32,
    peanut_allergy: bool,

    league_team_id: Uuid,
    tournament_team_id: Option<Uuid>,
    deceased: bool,
    evolution: i32,
    items: Vec<Item>,
    state: PlayerState,
    hit_streak: i32,
    consecutive_hits: i32,

    game_attr: Vec<String>,
    week_attr: Vec<String>,
    seas_attr: Vec<String>,
    item_attr: Vec<String>,
    perm_attr: Vec<String>,

    buoyancy: Ranged<f32>,
    cinnamon: Ranged<f32>,
    coldness: Ranged<f32>,
    chasiness: Ranged<f32>,
    divinity: Ranged<f32>,
    martyrdom: Ranged<f32>,
    base_thirst: Ranged<f32>,
    indulgence: Ranged<f32>,
    musclitude: Ranged<f32>,
    tragicness: Ranged<f32>,
    omniscience: Ranged<f32>,
    patheticism: Ranged<f32>,
    suppression: Ranged<f32>,
    continuation: Ranged<f32>,
    ruthlessness: Ranged<f32>,
    watchfulness: Ranged<f32>,
    laserlikeness: Ranged<f32>,
    overpowerment: Ranged<f32>,
    tenaciousness: Ranged<f32>,
    thwackability: Ranged<f32>,
    anticapitalism: Ranged<f32>,
    ground_friction: Ranged<f32>,
    pressurization: Ranged<f32>,
    unthwackability: Ranged<f32>,
    shakespearianism: Ranged<f32>,
    moxie: Ranged<f32>,
    total_fingers: i32,

    defense_rating: f32,
    hitting_rating: f32,
    pitching_rating: f32,
    baserunning_rating: f32,
}

impl Entity for Player {
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
}

impl Player {
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