use serde::Deserialize;
use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};
use crate::ingest::sim::{Entity, FeedEventChangeResult};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Item {

}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlayerState {

}

#[derive(Deserialize)]
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

    buoyancy: f32,
    cinnamon: f32,
    coldness: f32,
    chasiness: f32,
    divinity: f32,
    martyrdom: f32,
    base_thirst: f32,
    indulgence: f32,
    musclitude: f32,
    tragicness: f32,
    omniscience: f32,
    patheticism: f32,
    suppression: f32,
    continuation: f32,
    ruthlessness: f32,
    watchfulness: f32,
    laserlikeness: f32,
    overpowerment: f32,
    tenaciousness: f32,
    thwackability: f32,
    anticapitalism: f32,
    ground_friction: f32,
    pressurization: f32,
    unthwackability: f32,
    shakespearianism: f32,
    moxie: f32,
    total_fingers: f32,

    defense_rating: f32,
    hitting_rating: f32,
    pitching_rating: f32,
    baserunning_rating: f32,
}

impl Entity for Player {
    fn apply_event(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
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

    fn could_be(&self, other: &Self) -> bool {
        todo!()
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