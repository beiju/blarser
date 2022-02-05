use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;
use partial_information::{Ranged, PartialInformationCompare, MaybeKnown};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::event_utils;
use crate::sim::{Entity, FeedEventChangeResult};
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
pub struct Item {}

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
pub struct PlayerState {}

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
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

    pub defense_rating: MaybeKnown<f32>,
    pub hitting_rating: MaybeKnown<f32>,
    pub pitching_rating: MaybeKnown<f32>,
    pub baserunning_rating: MaybeKnown<f32>,
}

impl Entity for Player {
    fn name() -> &'static str {
        "player"
    }

    fn next_timed_event(&self, _from_time: DateTime<Utc>, _to_time: DateTime<Utc>, _state: &StateInterface) -> Option<GenericEvent> {
        None
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
            EventType::Hit | EventType::HomeRun => {
                assert_eq!(&self.id, event_utils::get_one_id(&event.player_tags, "playerTags"),
                           "Can't apply Hit/HomeRun event to this player: Unexpected ID");
                self.consecutive_hits += 1;
                FeedEventChangeResult::Ok
            }
            EventType::FlyOut => {
                self.fielding_out(event, "flyout")
            }
            EventType::GroundOut => {
                self.fielding_out(event, "ground out")
            }
            EventType::PlayerStatReroll => {
                // This event is normally a child (or in events that use siblings, a non-first
                // sibling), but for Snow events it's a top-level event. For now I assert that it's
                // always snow.

                assert_eq!(event.description, format!("Snow fell on {}!", self.name),
                           "Unexpected top-level PlayerStatReroll event");

                // TODO: Find the actual range
                self.adjust_attributes(Ranged::Range(-0.025, 0.025));

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

    fn adjust_attributes(&mut self, range: Ranged<f32>) {
        self.adjust_batting(range);
        self.adjust_pitching(range);
        self.adjust_baserunning(range);
        self.adjust_defense(range);
    }

    fn adjust_batting(&mut self, range: Ranged<f32>) {
        self.buoyancy += range;
        self.divinity += range;
        self.martyrdom += range;
        self.moxie += range;
        self.musclitude += range;
        self.patheticism += range;
        self.thwackability += range;
        self.tragicness += range;

        self.hitting_rating = MaybeKnown::Unknown;
    }

    fn adjust_pitching(&mut self, range: Ranged<f32>) {
        self.coldness += range;
        self.overpowerment += range;
        self.ruthlessness += range;
        self.shakespearianism += range;
        self.suppression += range;
        self.unthwackability += range;

        self.pitching_rating = MaybeKnown::Unknown;
    }

    fn adjust_baserunning(&mut self, range: Ranged<f32>) {
        self.base_thirst += range;
        self.continuation += range;
        self.ground_friction += range;
        self.indulgence += range;
        self.laserlikeness += range;

        self.baserunning_rating = MaybeKnown::Unknown;
    }

    fn adjust_defense(&mut self, range: Ranged<f32>) {
        self.anticapitalism += range;
        self.chasiness += range;
        self.omniscience += range;
        self.tenaciousness += range;
        self.watchfulness += range;

        self.defense_rating = MaybeKnown::Unknown;
    }
}