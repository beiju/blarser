use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::ingest::sim::{Entity, FeedEventChangeResult, GenericEvent, EventType as GenericEventType};

#[derive(Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GameState {}

#[derive(Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Stadium {}

#[derive(Deserialize, PartialInformationCompare)]
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

#[derive(Deserialize, PartialInformationCompare)]
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
    away_odds: Option<f32>,
    away_outs: i32,
    away_team: Uuid,
    end_phase: i32,
    home_odds: Option<f32>,
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
    play_count: i32,
    stadium_id: Option<Stadium>,
    statsheet: Uuid,
    at_bat_balls: i32,
    away_batter: Option<Uuid>,
    home_batter: Option<Uuid>,
    last_update: String,
    tournament: i32,
    away_pitcher: Option<Uuid>,
    away_strikes: Option<i32>,
    base_runners: Vec<Uuid>,
    home_pitcher: Option<Uuid>,
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
    away_pitcher_name: Option<String>,
    base_runner_names: Vec<String>,
    baserunner_count: i32,
    half_inning_score: i32,
    home_pitcher_name: Option<String>,
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
    fn apply_event(&mut self, event: &GenericEvent) -> FeedEventChangeResult {
        match &event.event_type {
            other => {
                panic!("{:?} event does not apply to Game", other)
            }
        }
    }
}