use std::iter;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use rocket::{info, State};
use serde::Deserialize;
use serde_with::with_prefix;
use uuid::Uuid;
use partial_information::{PartialInformationCompare, Ranged, MaybeKnown};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::{api, event_utils};
use crate::sim::{Entity, FeedEventChangeResult, parse, Player, Team};
use crate::sim::parse::Base;
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GameState {
    // :/
    snowfall_events: MaybeKnown<Option<i32>>,

}

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
#[serde(rename_all = "PascalCase")] // it will be camelCase after being prefixed with "home"/"away"
#[allow(dead_code)]
pub struct GameByTeam {
    odds: Option<Ranged<f32>>,
    outs: i32,
    team: Uuid,
    balls: i32,
    bases: i32,
    score: Option<f32>,
    batter: Option<Uuid>,
    pitcher: Option<MaybeKnown<Uuid>>,
    strikes: Option<i32>,
    team_name: String,
    team_runs: Option<f32>,
    team_color: String,
    team_emoji: String,
    batter_mod: String,
    batter_name: Option<String>,
    pitcher_mod: String,
    pitcher_name: Option<MaybeKnown<String>>,
    team_nickname: String,
    team_batter_count: Option<i32>,
    team_secondary_color: String,
}

#[derive(Clone, Deserialize, PartialInformationCompare)]
// Can't use deny_unknown_fields here because of the prefixed subobjects
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Game {
    id: Uuid,
    day: i32,
    sim: String,
    loser: Option<Uuid>,
    // I think?
    phase: i32,
    rules: Uuid,
    shame: bool,
    state: GameState,
    inning: i32,
    season: i32,
    winner: Option<Uuid>,
    // I think?
    weather: i32,
    end_phase: i32,
    outcomes: Vec<String>,
    season_id: Uuid,
    finalized: bool,
    game_start: bool,
    play_count: i64,
    stadium_id: Option<Uuid>,
    statsheet: Uuid,
    at_bat_balls: i32,
    last_update: String,
    tournament: i32,
    base_runners: Vec<Uuid>,
    repeat_count: i32,
    score_ledger: String,
    score_update: String,
    series_index: i32,
    terminology: Uuid,
    top_of_inning: bool,
    at_bat_strikes: i32,
    game_complete: bool,
    is_postseason: bool,
    is_prize_match: Option<bool>,
    is_title_match: bool,
    queued_events: Vec<i32>,
    // what? (i put i32 there as a placeholder)
    series_length: i32,
    bases_occupied: Vec<Ranged<i32>>,
    base_runner_mods: Vec<String>,
    game_start_phase: i32,
    half_inning_outs: i32,
    last_update_full: Option<Vec<UpdateFull>>,
    new_inning_phase: i32,
    top_inning_score: i32,
    base_runner_names: Vec<String>,
    baserunner_count: i32,
    half_inning_score: i32,
    tournament_round: Option<i32>,
    // what? (i32 placeholder)
    secret_baserunner: Option<Uuid>,
    bottom_inning_score: i32,
    new_half_inning_phase: i32,
    tournament_round_game_index: Option<i32>,

    #[serde(flatten, with = "prefix_home")]
    home: GameByTeam,

    #[serde(flatten, with = "prefix_away")]
    away: GameByTeam,
}

with_prefix!(prefix_home "home");
with_prefix!(prefix_away "away");


impl Entity for Game {
    fn name() -> &'static str {
        "game"
    }

    fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult {
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

struct ActivePitcher {
    rotation_slot: i64,
    pitcher_id: Uuid,
    pitcher_name: String,
}

struct Score {
    player_name: String,
    source: &'static str,
    runs: i64, // falsehoods
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

        let result = match event.r#type {
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
            other => {
                panic!("{:?} event does not apply to Game", other)
            }
        };
        // During Snow games, sometimes snowfall_events gets changed from null to 0. I can't figure
        // out what triggers it, so I'm just saying that if it's currently null, and the weather is
        // Snowy, any event might set it to 0
        if let FeedEventChangeResult::Ok = result {
            if let MaybeKnown::Known(None) = self.state.snowfall_events {
                // There's probably an easier way but hey, this works
                let weather: api::Weather = serde_json::from_value(serde_json::json!(self.weather))
                    .expect("Unexpected Weather type");
                if weather == api::Weather::Snowy {
                    self.state.snowfall_events = MaybeKnown::Unknown;
                }
            }
        }

        result
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
        self.half_inning_score = 0;

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
        FeedEventChangeResult::Ok
    }

    fn lets_go(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        self.game_start = true;
        self.game_start_phase = -1;
        self.home.team_batter_count = Some(-1);
        self.away.team_batter_count = Some(-1);

        FeedEventChangeResult::Ok
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
                phase: event.phase, // todo ?
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

        info!("At bat team has batter: {}", self.team_at_bat().batter.is_some());
        info!("Fielding team has batter: {}", self.team_fielding().batter.is_some());
        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during GroundOut/FlyOut event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during GroundOut/FlyOut event");

        // Verify batter id if the event has the player id; annoyingly, sometimes it doesn't
        if let Some(event_batter_id) = event.player_tags.first() {
            assert_eq!(event_batter_id, &batter_id,
                       "Batter in GroundOut/Flyout event didn't match batter in game state");
        }

        let (scoring_runners, other_events) = separate_scoring_events(&event.metadata.siblings, &batter_id);

        let out = match other_events.len() {
            1 => parse::parse_simple_out(&batter_name, &other_events[0].description)
                .expect("Error parsing simple fielding out"),
            2 => parse::parse_complex_out(&batter_name, &other_events[0].description, &other_events[1].description)
                .expect("Error parsing complex fielding out"),
            more => panic!("Unexpected fielding out with {} non-score siblings", more)
        };

        for runner_id in scoring_runners {
            let source = if let parse::FieldingOut::FieldersChoice(_, _) = out {
                "Base Hit"
            } else {
                "Sacrifice"
            };
            self.score_runner(runner_id, source);
        }

        let outs_added = if let parse::FieldingOut::DoublePlay = out { 2 } else { 1 };
        self.out(event, outs_added);
        self.end_at_bat();

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
            } else if self.half_inning_outs < 3 {
                // Need to figure out how to handle double plays with multiple people on base
                todo!()
            }
            self.advance_runners(0);
        } else {
            self.advance_runners(0);
        }

        FeedEventChangeResult::Ok
    }

    fn score_runner(&mut self, runner_id: &Uuid, source: &'static str) -> Score {
        let runner_from_state = self.base_runners.remove(0);
        if runner_from_state != *runner_id {
            panic!("Got a scoring event for {} but {} was first in the list", runner_id, runner_from_state);
        }
        let runner_name = self.base_runner_names.remove(0);
        self.base_runner_mods.remove(0);
        self.bases_occupied.remove(0);
        self.baserunner_count -= 1;

        Score {
            player_name: runner_name,
            source,
            runs: 1,
        }
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
                self.top_inning_score = 0;
                self.bottom_inning_score = 0;
                self.half_inning_score = 0;
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
                    self.top_inning_score = 0;
                    self.half_inning_score = 0;
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
            .expect("Couldn't find baserunner with specified on specified base")
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

        let (scoring_runners, _) = separate_scoring_events(&event.metadata.siblings, &batter_id);
        for runner_id in scoring_runners {
            self.score_runner(runner_id, "Base Hit");
        }

        self.game_event(event);
        self.advance_runners(hit_type as i32 + 1);
        let batter_mod = self.team_at_bat().batter_mod.clone();
        self.push_base_runner(batter_id, batter_name, batter_mod, hit_type);
        self.end_at_bat();

        FeedEventChangeResult::Ok
    }

    fn home_run(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult {
        let event_batter_id = event_utils::get_one_id(&event.player_tags, "playerTags");
        let batter_id = self.team_at_bat().batter.clone()
            .expect("Batter must exist during HomeRun event");
        let batter_name = self.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during HomeRun event");

        assert_eq!(event_batter_id, &batter_id,
                   "Batter in HomeRun event didn't match batter in game state");

        parse::parse_home_run(&batter_name, &event.description)
            .expect("Error parsing HomeRun description");

        for runner_id in self.base_runners.clone() {
            self.score_runner(&runner_id, "Home Run");
        }

        self.game_event(event);
        self.end_at_bat();

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
}


fn separate_scoring_events<'a>(siblings: &'a Vec<EventuallyEvent>, hitter_id: &'a Uuid) -> (Vec<&'a Uuid>, Vec<&'a EventuallyEvent>) {
    // The first event is never a scoring event, and it mixes up the rest of the logic because the
    // "hit" or "walk" event type is reused
    let (first, rest) = siblings.split_first()
        .expect("Event's siblings array is empty");
    let mut scores = Vec::new();
    let mut others = vec![first];

    for event in rest {
        if event.r#type == EventType::Hit || event.r#type == EventType::Walk {
            scores.push(event_utils::get_one_id_excluding(&event.player_tags, "playerTags", Some(hitter_id)));
        } else if event.r#type != EventType::RunsScored {
            others.push(event);
        }
    }

    (scores, others)
}