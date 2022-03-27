use std::fmt::{Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::with_prefix;
use uuid::Uuid;
use partial_information::{PartialInformationCompare, MaybeKnown};
use partial_information_derive::PartialInformationCompare;

use crate::entity::{AnyEntity, Base, Entity, EntityRaw, RunnerAdvancement, WrongEntityError};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GameState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snowfall_events: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
#[partial_information(default)]
pub struct UpdateFullMetadata {
    r#mod: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct UpdateFull {
    pub id: Uuid,
    pub day: i32,
    pub nuts: i32,
    pub r#type: i32,
    pub blurb: String,
    pub phase: i32,
    pub season: i32,
    pub created: DateTime<Utc>,
    pub category: i32,
    #[serde(default)]
    pub metadata: UpdateFullMetadata,
    pub game_tags: Vec<Uuid>,
    pub team_tags: Vec<Uuid>,
    pub player_tags: Vec<Uuid>,
    pub tournament: i32,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")] // it will be camelCase after being prefixed with "home"/"away"
#[allow(dead_code)]
pub struct GameByTeam {
    pub odds: Option<MaybeKnown<f32>>,
    pub outs: i32,
    pub team: Uuid,
    pub balls: i32,
    pub bases: i32,
    pub score: Option<f32>,
    pub batter: Option<Uuid>,
    pub pitcher: Option<MaybeKnown<Uuid>>,
    pub strikes: Option<i32>,
    pub team_name: String,
    pub team_runs: Option<f32>,
    pub team_color: String,
    pub team_emoji: String,
    pub batter_mod: String,
    pub batter_name: Option<String>,
    pub pitcher_mod: String,
    pub pitcher_name: Option<MaybeKnown<String>>,
    pub team_nickname: String,
    pub team_batter_count: Option<i32>,
    pub team_secondary_color: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
// Can't use deny_unknown_fields here because of the prefixed sub-objects
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Game {
    pub id: Uuid,
    pub day: i32,
    pub sim: String,
    pub loser: Option<Uuid>,
    pub phase: i32,
    pub rules: Uuid,
    pub shame: bool,
    pub state: GameState,
    pub inning: i32,
    pub season: i32,
    pub winner: Option<Uuid>,
    pub weather: i32,
    pub end_phase: i32,
    pub outcomes: Vec<String>,
    pub season_id: Uuid,
    pub finalized: bool,
    pub game_start: bool,
    pub play_count: i64,
    pub stadium_id: Option<Uuid>,
    pub statsheet: Uuid,
    pub at_bat_balls: i32,
    pub last_update: String,
    pub tournament: i32,
    pub base_runners: Vec<Uuid>,
    pub repeat_count: i32,
    pub score_ledger: String,
    pub score_update: String,
    pub series_index: i32,
    pub terminology: Uuid,
    pub top_of_inning: bool,
    pub at_bat_strikes: i32,
    pub game_complete: bool,
    pub is_postseason: bool,
    pub is_prize_match: Option<bool>,
    pub is_title_match: bool,
    pub queued_events: Vec<i32>,
    pub series_length: i32,
    pub bases_occupied: Vec<i32>,
    pub base_runner_mods: Vec<String>,
    pub game_start_phase: i32,
    pub half_inning_outs: i32,
    pub last_update_full: Option<Vec<UpdateFull>>,
    pub new_inning_phase: i32,
    pub top_inning_score: f32,
    pub base_runner_names: Vec<String>,
    pub baserunner_count: i32,
    pub half_inning_score: f32,
    pub tournament_round: Option<i32>,
    pub secret_baserunner: Option<Uuid>,
    pub bottom_inning_score: f32,
    pub new_half_inning_phase: i32,
    pub tournament_round_game_index: Option<i32>,

    #[serde(flatten, with = "prefix_home")]
    pub home: GameByTeam,

    #[serde(flatten, with = "prefix_away")]
    pub away: GameByTeam,
}

with_prefix!(prefix_home "home");
with_prefix!(prefix_away "away");

impl Display for Game {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Game: {} @ {}", self.away.team_name, self.home.team_name)
    }
}

impl Into<AnyEntity> for Game {
    fn into(self) -> AnyEntity {
        AnyEntity::Game(self)
    }
}

impl TryFrom<AnyEntity> for Game {
    type Error = WrongEntityError;

    fn try_from(value: AnyEntity) -> Result<Self, Self::Error> {
        match value {
            AnyEntity::Game(value) => { Ok(value) }
            other => Err(WrongEntityError { expected: "game", found: other.name() })
        }
    }
}

impl Entity for Game {
    fn name() -> &'static str { "game" }
    fn id(&self) -> Uuid { self.id }
}

impl EntityRaw for <Game as PartialInformationCompare>::Raw {
    type Entity = Game;

    fn name() -> &'static str { "game" }
    fn id(&self) -> Uuid { self.id }

    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        // If there's a lastUpdateFull, we know exactly when it was from
        if let Some(luf) = &self.last_update_full {
            if let Some(event) = luf.first() {
                return event.created;
            }
        }

        // Otherwise, games are timestamped from after the fetch
        valid_from - Duration::minutes(1)
    }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        // If there's a lastUpdateFull, we know exactly when it was from
        if let Some(luf) = &self.last_update_full {
            if let Some(event) = luf.first() {
                return event.created;
            }
        }

        // Otherwise, games are timestamped from after the fetch
        valid_from
    }
}

impl Game {
    pub(crate) fn team_at_bat(&self) -> &GameByTeam {
        if self.top_of_inning {
            &self.away
        } else {
            &self.home
        }
    }

    pub(crate) fn team_at_bat_mut(&mut self) -> &mut GameByTeam {
        if self.top_of_inning {
            &mut self.away
        } else {
            &mut self.home
        }
    }

    pub(crate) fn team_fielding(&self) -> &GameByTeam {
        if self.top_of_inning {
            &self.home
        } else {
            &self.away
        }
    }

    pub(crate) fn team_fielding_mut(&mut self) -> &mut GameByTeam {
        if self.top_of_inning {
            &mut self.home
        } else {
            &mut self.away
        }
    }

    // pub(crate) fn score_runner(&mut self, runner_id: Uuid) {
    //     let runner_from_state = self.base_runners.remove(0);
    //     if runner_from_state != runner_id {
    //         panic!("Got a scoring event for {} but {} was first in the list", runner_id, runner_from_state);
    //     }
    //     self.base_runner_names.remove(0);
    //     self.base_runner_mods.remove(0);
    //     self.bases_occupied.remove(0);
    //     self.baserunner_count -= 1;
    // }
    //
    pub(crate) fn out(&mut self, outs_added: i32) {
        let end_of_half_inning = self.half_inning_outs + outs_added == 3;
        if end_of_half_inning {
            self.half_inning_outs = 0;
            self.phase = 3;
            self.clear_bases();

            // Reset both top and bottom inning scored only when the bottom half ends
            if !self.top_of_inning {
                self.top_inning_score = 0.0;
                self.bottom_inning_score = 0.0;
                self.half_inning_score = 0.0;
            }

            // End the game
            if self.game_should_end() {
                self.top_inning_score = 0.0;
                self.half_inning_score = 0.0;
                self.phase = 7;
            }
        } else {
            self.half_inning_outs += outs_added;
        }

        self.end_at_bat()
    }

    fn game_should_end(&self) -> bool {
        if self.inning < 8 { return false; }

        let home_score = self.home.score
            .expect("Score field must not be null during a game");
        let away_score = self.away.score
            .expect("Score field must not be null during a game");
        if self.top_of_inning {
            home_score > away_score
        } else {
            home_score != away_score // i can feel the spectre of 20.3
        }
    }

    pub fn clear_bases(&mut self) {
        self.base_runners.clear();
        self.base_runner_names.clear();
        self.base_runner_mods.clear();
        self.bases_occupied.clear();
        self.baserunner_count = 0;
    }

    pub(crate) fn end_at_bat(&mut self) {
        self.team_at_bat_mut().batter = None;
        self.team_at_bat_mut().batter_name = Some("".to_string());
        self.at_bat_balls = 0;
        self.at_bat_strikes = 0;
    }

    pub(crate) fn get_baserunner_with_name(&self, expected_name: &str, base_plus_one: Base) -> usize {
        self.get_baserunner_with_property(expected_name, base_plus_one, &self.base_runner_names)
            .expect("Couldn't find baserunner with specified name on specified base")
    }

    fn get_baserunner_with_id(&self, expected_id: Uuid, base_plus_one: Base) -> usize {
        self.get_baserunner_with_property(&expected_id, base_plus_one, &self.base_runners)
            .expect("Couldn't find baserunner with specified id on specified base")
    }

    fn get_baserunner_with_property<U, T: ?Sized + std::cmp::PartialEq<U>>(
        &self, expected_property: &T, which_base: Base, baserunner_properties: &[U],
    ) -> Option<usize> {
        Iterator::zip(baserunner_properties.into_iter(), self.bases_occupied.iter())
            .enumerate()
            .filter_map(|(i, (name, base))| {
                if expected_property == name && *base == (which_base as i32 - 1) {
                    Some(i)
                } else {
                    None
                }
            })
            .exactly_one().ok()
    }

    pub fn advance_runners(&mut self, advancements: &[RunnerAdvancement]) {
        for (i, advancement) in advancements.iter().enumerate() {
            assert_eq!(self.base_runners[i], advancement.runner_id);
            assert_eq!(self.bases_occupied[i], advancement.from_base);
            self.bases_occupied[i] = advancement.to_base;
        }
    }

    // pub(crate) fn remove_base_runner(&mut self, runner_idx: usize) {
    //     self.base_runners.remove(runner_idx);
    //     self.base_runner_names.remove(runner_idx);
    //     self.base_runner_mods.remove(runner_idx);
    //     self.bases_occupied.remove(runner_idx);
    //     self.baserunner_count -= 1;
    // }
    //
    // pub(crate) fn remove_each_base_runner(self) -> impl Iterator<Item=Self> {
    //     // Intended for cases when we know some base runner got out, but we don't know which (i.e.,
    //     // double plays)
    //     let num_base_runners = self.bases_occupied.len();
    //     assert!(num_base_runners > 0, "Tried to remove a baserunner when there weren't any");
    //     iter::repeat(self)
    //         .take(num_base_runners)
    //         .enumerate()
    //         .map(|(i, mut game)| {
    //             game.remove_base_runner(i);
    //             game
    //         })
    // }
    //
    //
    pub(crate) fn push_base_runner(&mut self, runner_id: Uuid, runner_name: String, runner_mod: String, to_base: Base) {
        self.base_runners.push(runner_id);
        self.base_runner_names.push(runner_name);
        self.base_runner_mods.push(runner_mod);
        self.bases_occupied.push(to_base as i32);
        self.baserunner_count += 1;

        let mut last_occupied_base: Option<i32> = None;
        for base_num in self.bases_occupied.iter_mut().rev() {
            if let Some(last_occupied_base_num) = last_occupied_base.as_mut() {
                if *base_num <= *last_occupied_base_num {
                    *last_occupied_base_num = *base_num + 1;

                    *base_num = *last_occupied_base_num;
                } else {
                    *last_occupied_base_num = *base_num;
                }
            } else {
                last_occupied_base = Some(*base_num);
            }
        }
    }
    //
    // pub(crate) fn apply_successful_steal(&mut self, event: &EventuallyEvent, thief_id: Uuid, base: Base) {
    //     let baserunner_index = self.get_baserunner_with_id(thief_id, base);
    //
    //     if let Base::Fourth = base {
    //         self.score_runner(thief_id);
    //     } else {
    //         self.bases_occupied[baserunner_index] += 1;
    //     }
    //
    //     self.game_update_pitch(event);
    // }
    //
    // pub(crate) fn apply_caught_stealing(&mut self, event: &EventuallyEvent, thief_id: Uuid, base: Base) {
    //     let baserunner_index = self.get_baserunner_with_id(thief_id, base);
    //     self.remove_base_runner(baserunner_index);
    //
    //     self.game_update_pitch(event);
    //
    //     self.half_inning_outs += 1;
    //     if self.half_inning_outs >= self.team_at_bat().outs {
    //         self.end_at_bat();
    //         // Weird thing the game does when the inning ends but the PA doesn't
    //         *self.team_at_bat_mut().team_batter_count.as_mut()
    //             .expect("Team batter count must not be null during a CaughtStealing event") -= 1;
    //         self.clear_bases();
    //         self.phase = if self.game_should_end() { 7 } else { 3 };
    //         self.half_inning_outs = 0;
    //
    //         // Reset both top and bottom inning scored only when the bottom half ends
    //         if !self.top_of_inning {
    //             self.top_inning_score = 0.0;
    //             self.bottom_inning_score = 0.0;
    //             self.half_inning_score = 0.0;
    //         }
    //     }
    // }
}