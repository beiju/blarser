use std::collections::HashSet;
use std::fmt::format;
use std::iter;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use serde::Deserialize;
use serde_with::with_prefix;
use uuid::Uuid;
use partial_information::{Cached, MaybeKnown, PartialInformationCompare, Ranged};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::event_utils;
use crate::sim::{Entity, FeedEventChangeResult, parse, Player, Sim, Team};
use crate::sim::entity::EarliestEvent;
use crate::sim::parse::Base;
use crate::state::{GenericEvent, GenericEventType, StateInterface};

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GameState {
    snowfall_events: Cached<Option<i32>>,

}

#[derive(Clone, Debug, Deserialize, Default, PartialInformationCompare)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct UpdateFullMetadata {
    r#mod: Option<String>,

}

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
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

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
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

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
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
    pub bases_occupied: Vec<Ranged<i32>>,
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


impl Entity for Game {
    fn name() -> &'static str {
        "game"
    }

    fn next_timed_event(&self, from_time: DateTime<Utc>, to_time: DateTime<Utc>, state: &StateInterface) -> Option<GenericEvent> {
        let mut earliest = EarliestEvent::new();

        let sim: Sim = state.get_sim(from_time);
        earliest.push_opt(sim.get_earlseason_start(from_time, to_time));

        // There's a game update without a corresponding game event. It happens at the end of the
        // first half of each inning, and from a cursory look at the game event json it appears to
        // occur every time `phase == 3` and `top_of_inning == true`
        if self.phase == 3 && self.top_of_inning {
            let event_time = self.last_update_full.as_ref()
                .expect("lastUpdateFull must be populated when game phase is 3")
                .first()
                .expect("lastUpdateFull must be non-empty when game phase is 3")
                .created + Duration::seconds(5);
            if from_time < event_time && event_time <= to_time {
                earliest.push(GenericEvent {
                    time: event_time,
                    event_type: GenericEventType::EndTopHalf,
                })
            }
        }

        earliest.into_inner()
    }

    fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult {
        #[allow(unreachable_patterns)]
        match &event.event_type {
            GenericEventType::EarlseasonStart => {
                // This event generates odds and sets a bunch of properties
                for self_by_team in [&mut self.home, &mut self.away] {
                    self_by_team.batter_name = Some(String::new());
                    self_by_team.odds = Some(Ranged::Range(0.0, 1.0));
                    self_by_team.pitcher = Some(MaybeKnown::Unknown);
                    self_by_team.pitcher_name = Some(MaybeKnown::Unknown);
                    self_by_team.score = Some(0.0);
                    self_by_team.strikes = Some(3);
                }
                self.last_update = String::new();
                self.last_update_full = Some(Vec::new());
                FeedEventChangeResult::Ok
            }
            GenericEventType::EndTopHalf => {
                self.phase = 2;
                self.play_count += 1;
                self.last_update = String::new();
                self.last_update_full = Some(Vec::new());
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
                    return FeedEventChangeResult::DidNotApply;
                }
            }
            Err(_) => return FeedEventChangeResult::DidNotApply,
        };

        match event.r#type {
            EventType::LetsGo => self.lets_go(event),
            EventType::StormWarning => self.storm_warning(event),
            EventType::PlayBall => self.play_ball(event),
            EventType::HalfInning => self.half_inning(event, state),
            EventType::BatterUp => self.batter_up(event, state),
            EventType::Strike => self.strike(event),
            EventType::Ball => self.ball(event),
            EventType::FoulBall => self.foul_ball(event),
            EventType::Strikeout => self.strikeout(event),
            // It's easier to combine ground out and flyout types into one function
            EventType::GroundOut => self.fielding_out(event),
            EventType::FlyOut => self.fielding_out(event),
            EventType::Hit => self.hit(event),
            EventType::HomeRun => self.home_run(event),
            EventType::Snowflakes => self.snowflakes(event),
            EventType::StolenBase => self.stolen_base(event, state),
            EventType::Walk => self.walk(event),
            EventType::InningEnd => self.inning_end(event),
            EventType::BatterSkipped => self.batter_skipped(event),
            EventType::PeanutFlavorText => self.peanut_flavor_text(event),
            EventType::GameEnd => self.game_end(event),
            EventType::WinCollectedRegular => self.win_collected_regular(event),
            EventType::GameOver => self.game_over(event),
            other => {
                panic!("{:?} event does not apply to Game", other)
            }
        }
    }

    fn strike(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.at_bat_strikes += 1;
        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn ball(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.at_bat_balls += 1;
        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn foul_ball(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        if self.at_bat_strikes < 2 {
            self.at_bat_strikes += 1;
        }
        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn batter_up(&mut self, event: &EventuallyEvent, state: &StateInterface) -> FeedEventChangeResult {
        let self_by_team = self.team_at_bat();

        let team: Team = state.entity(self_by_team.team, event.created);
        let batter_count = 1 + self_by_team.team_batter_count
            .expect("Team batter count must be populated during a game");
        self_by_team.team_batter_count = Some(batter_count);
        let batter_id = team.batter_for_count(batter_count as usize);
        self_by_team.batter = Some(batter_id);
        let player: Player = state.entity(batter_id, event.created);
        self_by_team.batter_name = Some(player.name);

        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn half_inning(&mut self, event: &EventuallyEvent, state: &StateInterface) -> FeedEventChangeResult {
        self.top_of_inning = !self.top_of_inning;
        if self.top_of_inning {
            self.inning += 1;
        }
        self.phase = 6;
        self.half_inning_score = 0.0;

        // The first halfInning event re-sets the data that PlayBall clears
        if self.inning == 0 {
            for self_by_team in [&mut self.home, &mut self.away] {
                let team: Team = state.entity(self_by_team.team, event.created);
                let pitcher_id = team.active_pitcher();
                let pitcher: Player = state.entity(pitcher_id, event.created);
                self_by_team.pitcher = Some(MaybeKnown::Known(pitcher_id));
                self_by_team.pitcher_name = Some(MaybeKnown::Known(pitcher.name));
            }
        }


        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn play_ball(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_start_phase = 20;
        self.inning = -1;
        self.phase = 2;
        self.top_of_inning = false;

        // Yeah, it unsets pitchers. Why, blaseball.
        self.home.pitcher = None;
        self.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
        self.away.pitcher = None;
        self.away.pitcher_name = Some(MaybeKnown::Known(String::new()));

        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn storm_warning(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_start_phase = 11; // sure why not
        self.game_event(event);
        self.state.snowfall_events.set_cached(Some(0), event.created + Duration::minutes(5));
        FeedEventChangeResult::Ok
    }

    fn lets_go(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_start = true;
        self.game_start_phase = -1;
        self.home.team_batter_count = Some(-1);
        self.away.team_batter_count = Some(-1);

        self.game_event(event);

        FeedEventChangeResult::Ok
    }

    fn game_event(&mut self, first_event: &EventuallyEvent) {
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
    }

    fn team_at_bat(&mut self) -> &mut GameByTeam {
        if self.top_of_inning {
            &mut self.away
        } else {
            &mut self.home
        }
    }

    fn team_fielding(&mut self) -> &mut GameByTeam {
        if self.top_of_inning {
            &mut self.home
        } else {
            &mut self.away
        }
    }

    fn fielding_out(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        // Ground outs and flyouts are different event types, but the logic is so similar that it's
        // easier to combine them

        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during GroundOut/FlyOut event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during GroundOut/FlyOut event");

        // Verify batter id if the event has the player id; annoyingly, sometimes it doesn't
        if let Some(event_batter_id) = event.player_tags.first() {
            assert_eq!(event_batter_id, &batter_id,
                       "Batter in GroundOut/Flyout event didn't match batter in game state");
        }

        let (scoring_runners, other_events) = event_utils::separate_scoring_events(&event.metadata.siblings, &batter_id);

        let out = match other_events.len() {
            1 => parse::parse_simple_out(&batter_name, &other_events[0].description)
                .expect("Error parsing simple fielding out"),
            2 => parse::parse_complex_out(&batter_name, &other_events[0].description, &other_events[1].description)
                .expect("Error parsing complex fielding out"),
            more => panic!("Unexpected fielding out with {} non-score siblings", more)
        };

        let outs_added = if let parse::FieldingOut::DoublePlay = out { 2 } else { 1 };

        for runner_id in scoring_runners {
            self.score_runner(runner_id);
        }

        if let parse::FieldingOut::FieldersChoice(runner_name_parsed, out_at_base) = out {
            let runner_idx = self.get_baserunner_with_name(runner_name_parsed, out_at_base);
            self.remove_base_runner(runner_idx);
            // Advance runners first to ensure the batter is not allowed past first
            self.advance_runners(0);
            let batter_mod = self.team_at_bat().batter_mod.clone();
            self.push_base_runner(batter_id, batter_name, batter_mod, Base::First);
        } else if let parse::FieldingOut::DoublePlay = out {
            if self.baserunner_count == 1 {
                self.remove_base_runner(0);
            } else if self.half_inning_outs + 2 < 3 {
                // Need to figure out how to handle double plays with multiple people on base
                todo!()
            }
            self.advance_runners(0);
        } else {
            self.advance_runners(0);
        }

        self.out(event, outs_added);
        self.end_at_bat();

        FeedEventChangeResult::Ok
    }

    fn score_runner(&mut self, runner_id: &Uuid) {
        let runner_from_state = self.base_runners.remove(0);
        if runner_from_state != *runner_id {
            panic!("Got a scoring event for {} but {} was first in the list", runner_id, runner_from_state);
        }
        self.base_runner_names.remove(0);
        self.base_runner_mods.remove(0);
        self.bases_occupied.remove(0);
        self.baserunner_count -= 1;
    }

    fn out(&mut self, event: &EventuallyEvent, outs_added: i32) {
        self.game_event(event);

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

    fn end_at_bat(&mut self) {
        self.team_at_bat().batter = None;
        self.team_at_bat().batter_name = Some("".to_string());
        self.at_bat_balls = 0;
        self.at_bat_strikes = 0;
    }

    fn get_baserunner_with_name(&self, expected_name: &str, base_plus_one: Base) -> usize {
        self.get_baserunner_with_property(expected_name, base_plus_one, &self.base_runner_names)
            .expect("Couldn't find baserunner with specified name on specified base")
    }

    fn get_baserunner_with_id(&self, expected_id: &Uuid, base_plus_one: Base) -> usize {
        self.get_baserunner_with_property(expected_id, base_plus_one, &self.base_runners)
            .expect("Couldn't find baserunner with specified id on specified base")
    }

    fn get_baserunner_with_property<U, T: ?Sized + std::cmp::PartialEq<U>>(
        &self, expected_property: &T, which_base: Base, baserunner_properties: &[U],
    ) -> Option<usize> {
        Iterator::zip(baserunner_properties.into_iter(), self.bases_occupied.iter())
            .enumerate()
            .filter_map(|(i, (name, base))| {
                if expected_property == name && base.could_be(&(which_base as i32 - 1)) {
                    Some(i)
                } else {
                    None
                }
            })
            .exactly_one().ok()
    }

    fn remove_base_runner(&mut self, runner_idx: usize) {
        self.base_runners.remove(runner_idx);
        self.base_runner_names.remove(runner_idx);
        self.base_runner_mods.remove(runner_idx);
        self.bases_occupied.remove(runner_idx);
        self.baserunner_count -= 1;
    }

    fn advance_runners(&mut self, advance_at_least: i32) {
        for base in &mut self.bases_occupied {
            // You can advance by up to 1 "extra" base
            *base = base.clone() + Ranged::Range(advance_at_least, advance_at_least + 1)
        }
    }

    fn push_base_runner(&mut self, runner_id: Uuid, runner_name: String, runner_mod: String, to_base: Base) {
        self.base_runners.push(runner_id);
        self.base_runner_names.push(runner_name);
        self.base_runner_mods.push(runner_mod);
        self.bases_occupied.push(Ranged::Known(to_base as i32));
        self.baserunner_count += 1;

        let mut last_occupied_base: Option<i32> = None;
        for base in self.bases_occupied.iter_mut().rev() {
            match base {
                Ranged::Known(base_num) => {
                    if let Some(last_occupied_base_num) = last_occupied_base.as_mut() {
                        if *base_num <= *last_occupied_base_num {
                            *last_occupied_base_num = *base_num + 1;

                            *base = Ranged::Known(*last_occupied_base_num);
                        } else {
                            *last_occupied_base_num = *base_num;
                        }
                    } else {
                        last_occupied_base = Some(*base_num);
                    }
                }
                Ranged::Range(min_base, max_base) => {
                    if let Some(last_occupied_base_num) = last_occupied_base {
                        if *min_base <= last_occupied_base_num {
                            let last_occupied_base_num = *min_base + 1;

                            if last_occupied_base_num == *max_base {
                                // Then this has collapsed the possibilities
                                *base = Ranged::Known(last_occupied_base_num);
                            } else {
                                // Then this has just narrowed down the range
                                *min_base = last_occupied_base_num;
                            }
                            last_occupied_base = Some(last_occupied_base_num)
                        } else {
                            last_occupied_base = Some(*min_base);
                        }
                    } else {
                        last_occupied_base = Some(*min_base);
                    }
                }
            }
        }
    }

    fn hit(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        let event_batter_id = event_utils::get_one_id(&event.player_tags, "playerTags");
        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during Hit event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during Hit event");

        assert_eq!(event_batter_id, &batter_id,
                   "Batter in Hit event didn't match batter in game state");

        let hit_type = parse::parse_hit(&batter_name, &event.description)
            .expect("Error parsing Hit description");

        let (scoring_runners, _) = event_utils::separate_scoring_events(&event.metadata.siblings, &batter_id);
        for runner_id in scoring_runners {
            self.score_runner(runner_id);
        }

        self.game_event(event);
        self.advance_runners(hit_type as i32 + 1);
        let batter_mod = self.team_at_bat().batter_mod.clone();
        self.push_base_runner(batter_id, batter_name, batter_mod, hit_type);
        self.end_at_bat();

        FeedEventChangeResult::Ok
    }

    fn home_run(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_event(event);

        let event_batter_id = event_utils::get_one_id(&event.player_tags, "playerTags");
        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during HomeRun event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during HomeRun event");

        assert_eq!(event_batter_id, &batter_id,
                   "Batter in HomeRun event didn't match batter in game state");

        parse::parse_home_run(&batter_name, &event.description)
            .expect("Error parsing HomeRun description");

        self.end_at_bat();

        for runner_id in self.base_runners.clone() {
            self.score_runner(&runner_id);
        }

        FeedEventChangeResult::Ok
    }

    fn strikeout(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        let event_batter_id = event_utils::get_one_id(&event.player_tags, "playerTags");
        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during Strikeout event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during Strikeout event");

        assert_eq!(event_batter_id, &batter_id,
                   "Batter in Strikeout event didn't match batter in game state");

        // The result isn't used now, but it will be when double strikes are implemented
        parse::parse_strikeout(&batter_name, &event.description)
            .expect("Error parsing Strikeout description");

        self.out(event, 1);
        self.end_at_bat();

        FeedEventChangeResult::Ok
    }

    fn snowflakes(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        let (snow_event, _) = event.metadata.siblings.split_first()
            .expect("Snowflakes event is missing metadata.siblings");

        parse::parse_snowfall(&snow_event.description)
            .expect("Error parsing Snowflakes description");

        self.game_event(event);
        self.game_start_phase = 20;
        self.state.snowfall_events.update_uncached(|val| {
            Some(val.expect("snowfallEvents must be set in Snowflakes event") + 1)
        });

        let frozen_players: HashSet<_> = event.metadata.siblings.iter()
            .flat_map(|event| {
                if let Some(serde_json::Value::String(mod_name)) = event.metadata.other.get("mod") {
                    if mod_name == "FROZEN" {
                        return Some(event_utils::get_one_id(&event.player_tags, "playerTags"));
                    }
                }

                None
            })
            .collect();

        // The Player entity will take care of adding the Frozen mod, but the Game entity needs to
        // check if the current batter or pitcher just got Frozen
        if let Some(batter_id) = self.team_at_bat().batter {
            if frozen_players.contains(&batter_id) {
                self.team_at_bat().batter = None;
                self.team_at_bat().batter_name = Some("".to_string());
            }
        }

        if let Some(pitcher_id) = &self.team_at_bat().pitcher {
            let pitcher_id = pitcher_id.known()
                .expect("Pitcher must be Known in Snowfall event");

            if frozen_players.contains(pitcher_id) {
                self.team_at_bat().pitcher = None;
                self.team_at_bat().pitcher_name = Some("".to_string().into());
            }
        }

        FeedEventChangeResult::Ok
    }

    fn stolen_base(&mut self, event: &EventuallyEvent, state: &StateInterface) -> FeedEventChangeResult {
        let thief_id = event_utils::get_one_id(&event.player_tags, "playerTags");
        let thief: Player = state.entity(*thief_id, event.created);

        let steal = parse::parse_stolen_base(&thief.name, &event.description)
            .expect("Error parsing StolenBase description");

        match steal {
            parse::BaseSteal::Steal(base) => {
                self.apply_successful_steal(event, thief, base)
            }
            parse::BaseSteal::CaughtStealing(base) => {
                self.apply_caught_stealing(event, thief, base)
            }
        }
    }

    fn apply_successful_steal(&mut self, event: &EventuallyEvent, thief: Player, base: Base) -> FeedEventChangeResult {
        let baserunner_index = self.get_baserunner_with_id(&thief.id, base);

        if let Base::Fourth = base {
            self.score_runner(&thief.id);
        } else {
            self.bases_occupied[baserunner_index] += 1;
        }

        self.game_event(event);

        FeedEventChangeResult::Ok
    }

    fn apply_caught_stealing(&mut self, event: &EventuallyEvent, thief: Player, base: Base) -> FeedEventChangeResult {
        let baserunner_index = self.get_baserunner_with_id(&thief.id, base);
        self.remove_base_runner(baserunner_index);

        self.game_event(event);

        self.half_inning_outs += 1;
        if self.half_inning_outs >= self.team_at_bat().outs {
            self.end_at_bat();
            // Weird thing the game does when the inning ends but the PA doesn't
            *self.team_at_bat().team_batter_count.as_mut()
                .expect("Team batter count must not be null during a CaughtStealing event") -= 1;
            self.phase = 3;
            self.half_inning_outs = 0;
        }

        FeedEventChangeResult::Ok
    }

    fn walk(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        let event_batter_id = event_utils::get_one_id(&event.player_tags, "playerTags");
        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during Walk event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during Walk event");

        assert_eq!(event_batter_id, &batter_id,
                   "Batter in Walk event didn't match batter in game state");

        let (scoring_runners, _) = event_utils::separate_scoring_events(&event.metadata.siblings, &batter_id);

        for scoring_runner in scoring_runners {
            self.score_runner(scoring_runner);
        }

        let batter_mod = self.team_at_bat().batter_mod.clone();
        self.push_base_runner(batter_id, batter_name, batter_mod, Base::First);
        self.end_at_bat();
        self.game_event(&event);

        FeedEventChangeResult::Ok
    }

    fn inning_end(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_event(&event);
        self.phase = 2;

        FeedEventChangeResult::Ok
    }

    fn batter_skipped(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_event(event);
        *self.team_at_bat().team_batter_count.as_mut()
            .expect("TeamBatterCount must be populated during a game") += 1;

        FeedEventChangeResult::Ok
    }

    fn peanut_flavor_text(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_event(event);
        FeedEventChangeResult::Ok
    }

    fn game_end(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.phase = 7;
        self.end_phase = 3;

        self.game_event(event);

        FeedEventChangeResult::Ok
    }

    fn win_collected_regular(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.end_phase = 4;
        self.game_event(event);

        FeedEventChangeResult::Ok
    }

    fn game_over(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.end_phase = 5;
        self.finalized = true;
        self.game_complete = true;

        if self.home.score.unwrap() > self.away.score.unwrap() {
            self.winner = Some(self.home.team);
            self.loser = Some(self.away.team);
        } else {
            self.loser = Some(self.home.team);
            self.winner = Some(self.away.team);
        };

        self.game_event(event);

        FeedEventChangeResult::Ok
    }
}
