use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;
use partial_information::{Ranged, PartialInformationCompare, MaybeKnown};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::event_utils;
use crate::event_utils::{get_one_id, separate_scoring_events};
use crate::sim::{Entity, FeedEventChangeResult, parse};
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
            EventType::Strikeout => {
                // assert_eq!(&self.id, event_utils::get_one_id(&event.player_tags, "playerTags"),
                //            "Can't apply Strikeout event to this player: Unexpected ID");
                // self.consecutive_hits  = 0;
                // FeedEventChangeResult::Ok
                FeedEventChangeResult::DidNotApply
            }
            EventType::FlyOut | EventType::GroundOut => {
                self.fielding_out(event)
            }
            EventType::PlayerStatReroll => {
                // This event is normally a child (or in events that use siblings, a non-first
                // sibling), but for Snow events it's a top-level event. For now I assert that it's
                // always snow.

                assert_eq!(event.description, format!("Snow fell on {}!", self.name),
                           "Unexpected top-level PlayerStatReroll event");

                // TODO: Find the actual range
                self.adjust_attributes(Ranged::Range(-0.03, 0.03));

                FeedEventChangeResult::Ok
            }
            EventType::Snowflakes => {
                let event_applies = event.metadata.siblings.iter()
                    .any(|event| {
                        event.r#type == EventType::AddedMod &&
                            *get_one_id(&event.player_tags, "playerTags") == self.id
                    });
                assert!(event_applies, "Got Snowflakes event for player that doesn't apply");

                self.game_attr.push("FROZEN".to_string());

                FeedEventChangeResult::Ok
            }
            other => {
                panic!("{:?} event does not apply to Player", other)
            }
        }
    }

    fn fielding_out(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        let (_, other_events) = separate_scoring_events(&event.metadata.siblings, &self.id);

        let out = match other_events.len() {
            1 => parse::parse_simple_out(&self.name, &other_events[0].description),
            2 => parse::parse_complex_out(&self.name, &other_events[0].description, &other_events[1].description),
            more => panic!("Unexpected fielding out with {} non-score siblings", more)
        };

        // Assume that any parse error is because this isn't the correct batter, and not because of
        // unexpected text in the event. It's not ideal but the unexpected text will be found when
        // the game entity tries to parse it, so it should be ok.
        if out.is_ok() {
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