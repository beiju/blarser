use std::fmt::{Display, Formatter};
use std::iter;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::with_prefix;
use uuid::Uuid;
use partial_information::{MaybeKnown, PartialInformationCompare, Ranged};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent, Weather};
use crate::parse::{Base};
use crate::sim::{Entity};
use crate::sim::entity::{EarliestEvent, TimedEvent, TimedEventType};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GameState {
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
    id: Uuid,
    day: i32,
    nuts: i32,
    r#type: i32,
    blurb: String,
    phase: i32,
    season: i32,
    created: DateTime<Utc>,
    category: i32,
    #[serde(default)]
    metadata: UpdateFullMetadata,
    game_tags: Vec<Uuid>,
    team_tags: Vec<Uuid>,
    player_tags: Vec<Uuid>,
    tournament: i32,
    description: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "PascalCase")] // it will be camelCase after being prefixed with "home"/"away"
#[allow(dead_code)]
pub struct GameByTeam {
    pub odds: Option<Ranged<f32>>,
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

impl Entity for Game {
    fn name() -> &'static str { "game" }
    fn id(&self) -> Uuid { self.id }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<TimedEvent> {
        let mut earliest = EarliestEvent::new(after_time);

        // There's a game update without a corresponding game event. It happens at the end of the
        // first half of each inning, and from a cursory look at the game event json it appears to
        // occur every time `phase == 3` and `top_of_inning == true`
        if self.phase == 3 && self.top_of_inning {
            let event_time = self.last_update_full.as_ref()
                .expect("lastUpdateFull must be populated when game phase is 3")
                .first()
                .expect("lastUpdateFull must be non-empty when game phase is 3")
                .created + Duration::seconds(5);
            earliest.push(TimedEvent {
                time: event_time,
                event_type: TimedEventType::EndTopHalf(self.id),
            })
        }

        earliest.into_inner()
    }

    fn time_range_for_update(valid_from: DateTime<Utc>, raw: &Self::Raw) -> (DateTime<Utc>, DateTime<Utc>) {
        // If there's a lastUpdateFull, we know exactly when it was from
        if let Some(luf) = &raw.last_update_full {
            if let Some(event) = luf.first() {
                return (event.created, event.created);
            }
        }

        // Otherwise, games are timestamped from after the fetch
        (valid_from - Duration::minutes(1), valid_from)
    }
}

impl Game {
    pub(crate) fn game_update_pitch(&mut self, first_event: &EventuallyEvent) {
        self.game_update_common(first_event);

        if self.weather == (Weather::Snowy as i32) && self.state.snowfall_events.is_none() {
            self.state.snowfall_events = Some(0);
        }
    }

    pub(crate) fn game_update_common(&mut self, first_event: &EventuallyEvent) {
        let events = &first_event.metadata.siblings;

        // play and playCount are out of sync by exactly 1
        self.play_count = 1 + first_event.metadata.play
            .expect("Game event must have metadata.play");

        // last_update is all the descriptions of the sibling events, separated by \n, and with an
        // extra \n at the end
        self.last_update = events.iter()
            .map(|e| &e.description)
            // This is a too-clever way of getting the extra \n at the end
            .chain(iter::once(&String::new()))
            .join("\n");


        // last_update_full is a subset of the event
        self.last_update_full = Some(events.iter().map(|event| {
            let team_tags = match event.r#type {
                EventType::AddedMod | EventType::RunsScored | EventType::WinCollectedRegular => {
                    // There's a chance it's always the first id... but I doubt it. Probably have
                    // to check which team the player from playerTags is on
                    vec![*event.team_tags.first()
                        .expect("teamTags must be populated in AddedMod event")]
                }
                EventType::GameEnd => { event.team_tags.clone() }
                _ => Vec::new()
            };

            let metadata = serde_json::from_value(event.metadata.other.clone())
                .expect("Couldn't get metadata from event");
            UpdateFull {
                id: event.id,
                day: event.day,
                nuts: 0, // todo can I even get this?
                r#type: event.r#type as i32,
                blurb: String::new(), // todo ?
                phase: event.phase, // todo ?
                season: event.season,
                created: event.created,
                category: event.category,
                game_tags: Vec::new(),
                team_tags,
                player_tags: event.player_tags.clone(),
                tournament: event.tournament,
                description: event.description.clone(),
                metadata,
            }
        }).collect());

        let score_event = events.iter()
            .filter(|event| event.r#type == EventType::RunsScored)
            .at_most_one()
            .expect("Expected at most one RunsScored event");

        if let Some(score_event) = score_event {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct RunsScoredMetadata {
                ledger: String,
                update: String,
                away_score: f32,
                home_score: f32,
            }

            let runs_metadata: RunsScoredMetadata = serde_json::from_value(score_event.metadata.other.clone())
                .expect("Error parsing RunsScored event metadata");
            self.score_update = runs_metadata.update;
            self.score_ledger = runs_metadata.ledger;
            let home_scored = runs_metadata.home_score - self.home.score
                .expect("homeScore must exist during a game event");
            let away_scored = runs_metadata.away_score - self.away.score
                .expect("awayScore must exist during a game event");
            self.half_inning_score += home_scored + away_scored;
            if self.top_of_inning {
                self.top_inning_score += home_scored + away_scored;
            } else {
                self.bottom_inning_score += home_scored + away_scored;
            }
            self.home.score = Some(runs_metadata.home_score);
            self.away.score = Some(runs_metadata.away_score);
        } else {
            self.score_update = String::new();
            self.score_ledger = String::new();
        }

        // TODO This isn't going to properly handle un-shame
        if events.iter().any(|e| e.r#type == EventType::ShamingRun) {
            self.shame = true;
        }
    }

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

    pub(crate) fn score_runner(&mut self, runner_id: Uuid) {
        let runner_from_state = self.base_runners.remove(0);
        if runner_from_state != runner_id {
            panic!("Got a scoring event for {} but {} was first in the list", runner_id, runner_from_state);
        }
        self.base_runner_names.remove(0);
        self.base_runner_mods.remove(0);
        self.bases_occupied.remove(0);
        self.baserunner_count -= 1;
    }

    pub(crate) fn out(&mut self, event: &EventuallyEvent, outs_added: i32) {
        self.game_update_pitch(event);

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
            if self.inning >= 8 {
                let home_score = self.home.score
                    .expect("Score field must not be null during a game");
                let away_score = self.away.score
                    .expect("Score field must not be null during a game");
                let end_game = if self.top_of_inning && home_score > away_score {
                    true
                } else if !self.top_of_inning && home_score != away_score { // 20.3
                    true
                } else {
                    false
                };

                if end_game {
                    self.top_inning_score = 0.0;
                    self.half_inning_score = 0.0;
                    self.phase = 7;
                }
            }
        } else {
            self.half_inning_outs += outs_added;
        }
    }

    fn clear_bases(&mut self) {
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

    pub(crate) fn remove_base_runner(&mut self, runner_idx: usize) {
        self.base_runners.remove(runner_idx);
        self.base_runner_names.remove(runner_idx);
        self.base_runner_mods.remove(runner_idx);
        self.bases_occupied.remove(runner_idx);
        self.baserunner_count -= 1;
    }

    pub(crate) fn remove_each_base_runner(self) -> impl Iterator<Item=Self> {
        // Intended for cases when we know some base runner got out, but we don't know which (i.e.,
        // double plays)
        let num_base_runners = self.bases_occupied.len();
        assert!(num_base_runners > 0, "Tried to remove a baserunner when there weren't any");
        iter::repeat(self)
            .take(num_base_runners)
            .enumerate()
            .map(|(i, mut game)| {
                game.remove_base_runner(i);
                game
            })
    }

    pub(crate) fn advance_runners(mut self, advance_at_least: i32) -> Vec<Self> {
        // Start by advancing everyone by the minimum amount
        for base in &mut self.bases_occupied { *base += advance_at_least; }

        let num_bases_occupied = self.bases_occupied.len();
        let mut versions = vec![self];
        for i in (0..num_bases_occupied).rev() {
            // Can't modify versions if I iterate it in place, and I need to clone most of the
            // versions anyway, so might as well clone versions here
            for mut version in versions.clone() {
                let base = version.bases_occupied[i];

                // Don't add a version that involves players advancing to the same base as another
                // player (TODO: except circumstances known to cause handholding)
                if let Some(next_occupied_base) = version.bases_occupied.get(i + 1) {
                    if *next_occupied_base == base + 1 {
                        continue
                    }
                }

                version.bases_occupied[i] += 1;
                versions.push(version);
            }
        }

        versions
    }

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

    pub(crate) fn apply_successful_steal(&mut self, event: &EventuallyEvent, thief_id: Uuid, base: Base) {
        let baserunner_index = self.get_baserunner_with_id(thief_id, base);

        if let Base::Fourth = base {
            self.score_runner(thief_id);
        } else {
            self.bases_occupied[baserunner_index] += 1;
        }

        self.game_update_pitch(event);

    }

    pub(crate) fn apply_caught_stealing(&mut self, event: &EventuallyEvent, thief_id: Uuid, base: Base) {
        let baserunner_index = self.get_baserunner_with_id(thief_id, base);
        self.remove_base_runner(baserunner_index);

        self.game_update_pitch(event);

        self.half_inning_outs += 1;
        if self.half_inning_outs >= self.team_at_bat().outs {
            self.end_at_bat();
            // Weird thing the game does when the inning ends but the PA doesn't
            *self.team_at_bat_mut().team_batter_count.as_mut()
                .expect("Team batter count must not be null during a CaughtStealing event") -= 1;
            self.clear_bases();
            self.phase = 3;
            self.half_inning_outs = 0;

            // Reset both top and bottom inning scored only when the bottom half ends
            if !self.top_of_inning {
                self.top_inning_score = 0.0;
                self.bottom_inning_score = 0.0;
                self.half_inning_score = 0.0;
            }
        }

    }

}