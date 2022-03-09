use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{Ranged, PartialInformationCompare, MaybeKnown};
use partial_information_derive::PartialInformationCompare;

use crate::sim::{Entity, TimedEvent};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
pub struct Item {
    // TODO Implement Item, reinstate deny_unknown_fields
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PlayerState {
    pub cut_this_election: Option<bool>,
    pub necromancied_this_election: Option<bool>,
    pub redacted: Option<bool>,
    pub elsewhere: Option<PlayerElsewhereInfo>,
    // Detective hunches
    pub hunches: Option<Vec<i32>>,
    pub investigations: Option<i32>,
    // Original player for this Replica
    pub original: Option<Uuid>,
    pub perm_mod_sources: Option<HashMap<String, Vec<String>>>,
    pub seas_mod_sources: Option<HashMap<String, Vec<String>>>,
    pub game_mod_sources: Option<HashMap<String, Vec<String>>>,
    pub item_mod_sources: Option<HashMap<String, Vec<Uuid>>>,
    pub unscattered_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PlayerElsewhereInfo {
    pub day: i32,
    pub season: i32,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Player {
    pub id: Uuid,
    pub name: String,
    pub ritual: Option<String>,
    pub fate: Option<i32>,
    pub soul: i32,
    pub blood: Option<i32>,
    pub coffee: Option<i32>,
    pub peanut_allergy: Option<bool>,

    pub bat: Option<String>,
    pub armor: Option<String>,

    pub league_team_id: Option<Uuid>,
    pub tournament_team_id: Option<Uuid>,
    pub deceased: Option<bool>,
    pub evolution: Option<i32>,
    pub items: Option<Vec<Item>>,
    pub state: Option<PlayerState>,
    pub hit_streak: Option<i32>,
    pub consecutive_hits: Option<i32>,

    pub game_attr: Option<Vec<String>>,
    pub week_attr: Option<Vec<String>>,
    pub seas_attr: Option<Vec<String>>,
    pub item_attr: Option<Vec<String>>,
    pub perm_attr: Option<Vec<String>>,

    pub buoyancy: Ranged<f32>,
    pub cinnamon: Option<Ranged<f32>>,
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

    pub defense_rating: Option<MaybeKnown<f32>>,
    pub hitting_rating: Option<MaybeKnown<f32>>,
    pub pitching_rating: Option<MaybeKnown<f32>>,
    pub baserunning_rating: Option<MaybeKnown<f32>>,
}

impl Display for Player {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Player: {}",
               self.state.as_ref()
                   .and_then(|state| state.unscattered_name.as_ref())
                   .unwrap_or(&self.name))
    }
}


impl Entity for Player {
    fn name() -> &'static str {
        "player"
    }
    fn id(&self) -> Uuid { self.id }

    fn next_timed_event(&self, _: DateTime<Utc>) -> Option<TimedEvent> {
        None
    }

    fn time_range_for_update(valid_from: DateTime<Utc>, _: &Self::Raw) -> (DateTime<Utc>, DateTime<Utc>) {
        // Players are timestamped before the fetch
        (valid_from, valid_from + Duration::minutes(1))
    }
}

impl Player {
    // fn apply_feed_event(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
    //     match event.r#type {
    //         EventType::Hit | EventType::HomeRun => {
    //             assert_eq!(&self.id, event_utils::get_one_id(&event.player_tags, "playerTags"),
    //                        "Can't apply Hit/HomeRun event to this player: Unexpected ID");
    //             // TODO: Remove this after figuring out why it happens / adding a more robust
    //             //   system for handing unexpected events
    //             if event.id == Uuid::parse_str("f41bd0bd-9d8f-4852-82c6-2155703950a9").unwrap() {
    //                 *self.consecutive_hits.as_mut().expect("Everyone but Phantom Sixpack has this") = 0;
    //             } else {
    //                 *self.consecutive_hits.as_mut().expect("Everyone but Phantom Sixpack has this") += 1;
    //             }
    //             FeedEventChangeResult::Ok
    //         }
    //         EventType::Strikeout => {
    //             // assert_eq!(&self.id, event_utils::get_one_id(&event.player_tags, "playerTags"),
    //             //            "Can't apply Strikeout event to this player: Unexpected ID");
    //             // self.consecutive_hits  = 0;
    //             // FeedEventChangeResult::Ok
    //             FeedEventChangeResult::DidNotApply
    //         }
    //         EventType::FlyOut | EventType::GroundOut => {
    //             self.fielding_out(event)
    //         }
    //         EventType::PlayerStatReroll => {
    //             // This event is normally a child (or in events that use siblings, a non-first
    //             // sibling), but for Snow events it's a top-level event. For now I assert that it's
    //             // always snow.
    //
    //             assert_eq!(event.description, format!("Snow fell on {}!", self.name),
    //                        "Unexpected top-level PlayerStatReroll event");
    //
    //             // TODO: Find the actual range
    //             self.adjust_attributes(Ranged::Range(-0.03, 0.03),
    //                                    event.created + Duration::minutes(5));
    //
    //             FeedEventChangeResult::Ok
    //         }
    //         EventType::Snowflakes => {
    //             let event_applies = event.metadata.siblings.iter()
    //                 .any(|event| {
    //                     event.r#type == EventType::AddedMod &&
    //                         *get_one_id(&event.player_tags, "playerTags") == self.id
    //                 });
    //             assert!(event_applies, "Got Snowflakes event for player that doesn't apply");
    //
    //             self.game_attr.as_mut().expect("Everyone but Phantom Sixpack has this").push("FROZEN".to_string());
    //
    //             FeedEventChangeResult::Ok
    //         }
    //         EventType::ModExpires => {
    //             let mods: Vec<String> = serde_json::from_value(event.metadata.other.get("mods")
    //                 .expect("ModExpires event must have 'mods' property in metadata").clone())
    //                 .expect("Failed to parse 'mods' property in metadata");
    //             let type_i = event.metadata.other.get("type")
    //                 .expect("ModExpires event must have 'type' property in metadata").clone()
    //                 .as_i64()
    //                 .expect("Failed to parse 'type' property in metadata");
    //
    //             let list = match type_i {
    //                 0 => &mut self.perm_attr,
    //                 2 => &mut self.seas_attr,
    //                 3 => &mut self.game_attr,
    //                 4 => &mut self.item_attr,
    //                 i => panic!("Unexpected value {} for mod type", i),
    //             };
    //
    //             for mod_name in mods {
    //                 list.as_mut()
    //                     .expect("Everyone but Phantom Sixpack has this")
    //                     .retain(|m| m != &mod_name);
    //             }
    //
    //             FeedEventChangeResult::Ok
    //         }
    //         other => {
    //             panic!("{:?} event does not apply to Player", other)
    //         }
    //     }
    // }
    //
    // fn fielding_out(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
    //     let (_, other_events) = separate_scoring_events(&event.metadata.siblings, &self.id);
    //
    //     let out = match other_events.len() {
    //         1 => parse::parse_simple_out(&self.name, &other_events[0].description),
    //         2 => parse::parse_complex_out(&self.name, &other_events[0].description, &other_events[1].description),
    //         more => panic!("Unexpected fielding out with {} non-score siblings", more)
    //     };
    //
    //     // Assume that any parse error is because this isn't the correct batter, and not because of
    //     // unexpected text in the event. It's not ideal but the unexpected text will be found when
    //     // the game entity tries to parse it, so it should be ok.
    //     if out.is_ok() {
    //         *self.consecutive_hits.as_mut().expect("Everyone but Phantom Sixpack has this") = 0;
    //         FeedEventChangeResult::Ok
    //     } else {
    //         FeedEventChangeResult::DidNotApply
    //     }
    // }

    pub fn adjust_attributes(&mut self, range: Ranged<f32>) {
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

        *self.hitting_rating.as_mut().expect("Everyone but Phantom Sixpack has this") = MaybeKnown::Unknown;
    }

    fn adjust_pitching(&mut self, range: Ranged<f32>) {
        self.coldness += range;
        self.overpowerment += range;
        self.ruthlessness += range;
        self.shakespearianism += range;
        self.suppression += range;
        self.unthwackability += range;

        *self.pitching_rating.as_mut().expect("Everyone but Phantom Sixpack has this") = MaybeKnown::Unknown;
    }

    fn adjust_baserunning(&mut self, range: Ranged<f32>) {
        self.base_thirst += range;
        self.continuation += range;
        self.ground_friction += range;
        self.indulgence += range;
        self.laserlikeness += range;

        *self.baserunning_rating.as_mut().expect("Everyone but Phantom Sixpack has this") = MaybeKnown::Unknown;
    }

    fn adjust_defense(&mut self, range: Ranged<f32>) {
        self.anticapitalism += range;
        self.chasiness += range;
        self.omniscience += range;
        self.tenaciousness += range;
        self.watchfulness += range;

        *self.defense_rating.as_mut().expect("Everyone but Phantom Sixpack has this") = MaybeKnown::Unknown;
    }
}