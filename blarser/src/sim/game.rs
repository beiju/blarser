use std::iter;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::Deserialize;
use uuid::Uuid;
use partial_information::{PartialInformationCompare, Ranged, MaybeKnown};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::sim::{Entity, FeedEventChangeResult, Team};
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GameState {}

#[derive(Clone, Deserialize, PartialInformationCompare)]
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
    game_tags: Vec<Uuid>,
    team_tags: Vec<Uuid>,
    player_tags: Vec<Uuid>,
    tournament: i32,
    description: String,
}

#[derive(Clone, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Game {
    id: Uuid,
    day: i32,
    sim: String,
    loser: Option<Uuid>, // I think?
    phase: i32,
    rules: Uuid,
    shame: bool,
    state: GameState,
    inning: i32,
    season: i32,
    winner: Option<Uuid>, // I think?
    weather: i32,
    away_odds: Option<Ranged<f32>>,
    away_outs: i32,
    away_team: Uuid,
    end_phase: i32,
    home_odds: Option<Ranged<f32>>,
    home_outs: i32,
    home_team: Uuid,
    outcomes: Vec<String>,
    season_id: Uuid,
    away_balls: i32,
    away_bases: i32,
    away_score: Option<f32>,
    finalized: bool,
    game_start: bool,
    home_balls: i32,
    home_bases: i32,
    home_score: Option<f32>,
    play_count: i64,
    stadium_id: Option<Uuid>,
    statsheet: Uuid,
    at_bat_balls: i32,
    away_batter: Option<Uuid>,
    home_batter: Option<Uuid>,
    last_update: String,
    tournament: i32,
    away_pitcher: Option<MaybeKnown<Uuid>>,
    away_strikes: Option<i32>,
    base_runners: Vec<Uuid>,
    home_pitcher: Option<MaybeKnown<Uuid>>,
    home_strikes: Option<i32>,
    repeat_count: i32,
    score_ledger: String,
    score_update: String,
    series_index: i32,
    terminology: Uuid,
    top_of_inning: bool,
    at_bat_strikes: i32,
    away_team_name: String,
    away_team_runs: Option<f32>,
    game_complete: bool,
    home_team_name: String,
    home_team_runs: Option<i32>,
    is_postseason: bool,
    is_prize_match: Option<bool>,
    is_title_match: bool,
    queued_events: Vec<i32>, // what? (i put i32 there as a placeholder)
    series_length: i32,
    away_batter_mod: String,
    away_team_color: String,
    away_team_emoji: String,
    bases_occupied: Vec<i32>,
    home_batter_mod: String,
    home_team_color: String,
    home_team_emoji: String,
    away_batter_name: Option<String>,
    away_pitcher_mod: String,
    base_runner_mods: Vec<String>,
    game_start_phase: i32,
    half_inning_outs: i32,
    home_batter_name: Option<String>,
    home_pitcher_mod: String,
    last_update_full: Option<Vec<UpdateFull>>,
    new_inning_phase: i32,
    top_inning_score: i32,
    away_pitcher_name: Option<MaybeKnown<String>>,
    base_runner_names: Vec<String>,
    baserunner_count: i32,
    half_inning_score: i32,
    home_pitcher_name: Option<MaybeKnown<String>>,
    tournament_round: Option<i32>, // what? (i32 placeholder)
    away_team_nickname: String,
    home_team_nickname: String,
    secret_baserunner: Option<Uuid>,
    bottom_inning_score: i32,
    new_half_inning_phase: i32,
    away_team_batter_count: Option<i32>,
    home_team_batter_count: i32,
    away_team_secondary_color: String,
    home_team_secondary_color: String,
    tournament_round_game_index: Option<i32>
}

impl Entity for Game {
    fn name() -> &'static str {
        "game"
    }

    fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult {
        match &event.event_type {
            GenericEventType::EarlseasonStart => {
                // This event generates odds and sets a bunch of properties
                self.home_batter_name = Some(String::new());
                self.home_odds = Some(Ranged::Range(0.0, 0.1));
                self.home_pitcher = Some(MaybeKnown::Unknown);
                self.home_pitcher_name = Some(MaybeKnown::Unknown);
                self.home_score = Some(0.0);
                self.home_strikes = Some(3);
                // This half should be an exact copy of the Home half
                self.away_batter_name = Some(String::new());
                self.away_odds = Some(Ranged::Range(0.0, 1.0));
                self.away_pitcher = Some(MaybeKnown::Unknown);
                self.away_pitcher_name = Some(MaybeKnown::Unknown);
                self.away_score = Some(0.0);
                self.away_strikes = Some(3);
                FeedEventChangeResult::Ok
            }
            GenericEventType::FeedEvent(feed_event) => {
                self.apply_feed_event(feed_event, state)
            }
            other => {
                panic!("{:?} event does not apply to Game", other)
            }
        }
    }
}

impl Game {
    fn apply_feed_event(&mut self, event: &EventuallyEvent, state: &StateInterface) -> FeedEventChangeResult {
        match event.game_tags.iter().exactly_one() {
            Ok(game_id) => {
                if &self.id != game_id {
                    return FeedEventChangeResult::DidNotApply
                }
            }
            Err(_) => return FeedEventChangeResult::DidNotApply,
        };

        match event.r#type {
            EventType::LetsGo => {
                self.game_start = true;
                self.game_start_phase = -1;
                self.home_team_batter_count = -1;
                self.away_team_batter_count = Some(-1);
                self.game_event(event);
                FeedEventChangeResult::Ok
            }
            EventType::StormWarning => {
                self.last_update = "WINTER STORM WARNING\n".to_string();
                FeedEventChangeResult::Ok
            }
            EventType::PlayBall => {
                self.game_start_phase = 20;
                self.inning = -1;
                self.phase = 2;
                self.top_of_inning = false;

                // Yeah, it unsets pitchers. Why, blaseball.
                self.home_pitcher = None;
                self.home_pitcher_name = Some(MaybeKnown::Known(String::new()));
                self.home_pitcher = None;
                self.away_pitcher_name = Some(MaybeKnown::Known(String::new()));
                self.away_pitcher = None;

                self.game_event(event);
                FeedEventChangeResult::Ok
            }
            EventType::HalfInning => {
                self.top_of_inning = !self.top_of_inning;
                if self.top_of_inning {
                    self.inning += 1;
                }
                self.phase = 6;
                self.half_inning_score = 0;

                self.game_event(event);
                FeedEventChangeResult::Ok
            }
            EventType::BatterUp => {
                if self.top_of_inning {
                    let team: Team = state.entity(self.home_team, event.created);
                    let batter_count = self.away_team_batter_count
                        .expect("Team batter count must be populated during a game");
                    self.away_batter = Some(team.batter_for_count(batter_count as usize));
                }
                FeedEventChangeResult::Ok
            }
            other => {
                panic!("{:?} event does not apply to Game", other)
            }
        }
    }

    fn game_event(&mut self, first_event: &EventuallyEvent) {
        let events = &first_event.metadata.siblings;

        // These will be overwritten if there is a score
        self.score_update = String::new();
        self.score_ledger = String::new();

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
            UpdateFull {
                id: event.id,
                day: event.day,
                nuts: 0, // todo can I even get this?
                r#type: event.r#type as i32,
                blurb: String::new(), // todo ?
                phase: 0, // todo ?
                season: event.season,
                created: event.created,
                category: event.category,
                game_tags: event.game_tags.clone(),
                team_tags: event.team_tags.clone(),
                player_tags: event.player_tags.clone(),
                tournament: event.tournament,
                description: event.description.clone(),
            }
        }).collect())
    }

}