use std::cmp::min;
use std::iter;
use std::sync::Arc;
use anyhow::{anyhow, Context};
use rocket::{async_trait};
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use serde_json::{json, Value};
use uuid::Uuid;
use serde::Deserialize;

use crate::api::{eventually, EventuallyEvent, EventType, Weather};
use crate::blaseball_state as bs;
use crate::ingest::{IngestItem, BoxedIngestItem, IngestResult, IngestApplyResult, IngestError};
use crate::ingest::data_views::{DataView, EntityView, View};
use crate::ingest::log::IngestLogger;
use crate::ingest::text_parser::{FieldingOut, Base, StrikeType, parse_simple_out, parse_hit, parse_home_run, parse_snowfall, parse_strike, parse_strikeout, parse_stolen_base, parse_complex_out};

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=BoxedIngestItem> + Send>> {
    vec![
        Box::new(eventually::events(start)
            .map(|event| Box::new(event) as BoxedIngestItem))
    ]
}

#[async_trait]
impl IngestItem for EventuallyEvent {
    fn date(&self) -> DateTime<Utc> {
        self.created
    }

    fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestApplyResult {
        let apply_log = format!("Applying Feed event {} from {}: \"{}\"", self.id, self.created, self.description);
        log.debug(apply_log.clone())?;

        let result = match self.r#type {
            EventType::BigDeal => apply_big_deal(state, log, self),
            EventType::LetsGo => apply_lets_go(state, log, self),
            EventType::PlayBall => apply_play_ball(state, log, self),
            EventType::HalfInning => apply_half_inning(state, log, self),
            EventType::BatterUp => apply_batter_up(state, log, self),
            EventType::Strike => apply_strike(state, log, self),
            EventType::Ball => apply_ball(state, log, self),
            EventType::FoulBall => apply_foul_ball(state, log, self),
            EventType::GroundOut => apply_ground_out(state, log, self),
            EventType::Hit => apply_hit(state, log, self),
            EventType::Strikeout => apply_strikeout(state, log, self),
            EventType::FlyOut => apply_fly_out(state, log, self),
            EventType::StolenBase => apply_stolen_base(state, log, self),
            EventType::Walk => apply_walk(state, log, self),
            EventType::HomeRun => apply_home_run(state, log, self),
            EventType::StormWarning => apply_storm_warning(state, log, self),
            EventType::Snowflakes => apply_snowflakes(state, log, self),
            EventType::PlayerStatReroll => apply_player_stat_reroll(state, log, self),
            EventType::InningEnd => apply_inning_end(state, log, self),
            EventType::BatterSkipped => apply_batter_skipped(state, log, self),
            _ => todo!()
        };

        log.increment_parsed_events()?;

        result.context(apply_log)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlayMetadata {
    pub play: i64,
}


fn apply_big_deal(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Ignoring BigDeal event".to_string())?;
    Ok((state, Vec::new()))
}


fn apply_lets_go(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying LetsGo event for game {}", game_id))?;

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct LetsGoMetadata {
        pub home: Uuid,
        pub away: Uuid,
        pub stadium: Option<Uuid>,
        pub weather: Weather,
    }

    let metadata: LetsGoMetadata = serde_json::from_value(event.metadata.other.clone())?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));

    for team_id in [metadata.home, metadata.away] {
        let pitcher = get_active_pitcher(&state, team_id, event.day > 0)?;
        let team = data.get_team(&team_id);

        team.get("rotationSlot").set(pitcher.rotation_slot)?;
    }

    let game = data.get_game(game_id);
    game.get("gameStart").set(true)?;
    game.get("gameStartPhase").set(-1)?;
    game.get("homeTeamBatterCount").set(-1)?;
    game.get("awayTeamBatterCount").set(-1)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}

struct ActivePitcher {
    rotation_slot: i64,
    pitcher_id: Uuid,
    pitcher_name: String,
}

fn get_active_pitcher(state: &Arc<bs::BlaseballState>, team_id: Uuid, advance: bool) -> Result<ActivePitcher, bs::PathError> {
    let rotation = state.array_at(&bs::json_path!("team", team_id, "rotation"))?;
    let rotation_slot = state.int_at(&bs::json_path!("team", team_id, "rotationSlot"))?;
    let rotation_slot = if advance {
        (rotation_slot + 1) % rotation.len() as i64
    } else {
        rotation_slot
    };

    let pitcher_id = rotation.get(rotation_slot as usize)
        .expect("rotation_slot should always be valid here");

    let pitcher_id = pitcher_id.as_uuid()
        .map_err(|value| bs::PathError::UnexpectedType {
            path: bs::json_path!("team", team_id, "rotation", rotation_slot as usize),
            expected_type: "uuid",
            value,
        })?;

    let pitcher_name = state.string_at(&bs::json_path!("player", pitcher_id, "name"))?;

    Ok(ActivePitcher { rotation_slot, pitcher_id, pitcher_name })
}

fn apply_play_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying PlayBall event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    game_update(&game, &event.metadata.siblings, "Play ball!\n", play, &[])?;

    for prefix in ["home", "away"] {
        game.get(&format!("{}Pitcher", prefix)).set(bs::PrimitiveValue::Null)?;
        game.get(&format!("{}PitcherName", prefix)).set("")?;
    }

    game.get("gameStartPhase").set(20)?;
    game.get("inning").set(-1)?;
    game.get("phase").set(2)?;
    game.get("topOfInning").set(false)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}

fn prefixed(text: &'static str, top_of_inning: bool) -> String {
    let home_or_away = if top_of_inning { "away" } else { "home" };
    format!("{}{}", home_or_away, text)
}

fn inning_prefixed(text: &'static str, top_of_inning: bool) -> String {
    let home_or_away = if top_of_inning { "top" } else { "bottom" };
    format!("{}{}", home_or_away, text)
}

fn game_update<T: Into<String>>(game: &EntityView, events: &[EventuallyEvent], message: T, play: i64, scores: &[Score]) -> IngestResult<()> {
    game.get("lastUpdate").set(message.into())?;
    // play and playCount are out of sync by exactly 1
    game.get("playCount").set(play + 1)?;

    // lastUpdateFull is never logically connected to its previous value. Re-set it each time.
    game.get("lastUpdateFull").overwrite(Value::Array(events.iter()
        .map(|event| {
            let mut result = json!({
                        "blurb": "",
                        "category": event.category as i32,
                        "created": format_blaseball_date(event.created),
                        "day": event.day,
                        "description": event.description,
                        "gameTags": [],
                        "id": event.id,
                        "nuts": 0,
                        "phase": 2,
                        "playerTags": event.player_tags,
                        "season": event.season,
                        "teamTags": [],
                        "tournament": event.tournament,
                        "type": event.r#type as i32,
                    });

            if let Value::Object(obj) = &event.metadata.other {
                if !obj.is_empty() {
                    result["metadata"] = event.metadata.other.clone();
                }
            }

            result
        })
        .collect()))?;

    if scores.is_empty() {
        game.get("scoreLedger").set("")?;
        game.get("scoreUpdate").set("")?;
    } else {
        let score_total = scores.iter().fold(0, |total, score| total + score.runs);

        let score_expression = scores.iter()
            .map(|score| score.runs.to_string())
            .join(" + ");

        let mut score_ledger = scores.iter()
            .map(|score| {
                let plural = match score.runs {
                    1 => "",
                    _ => "s"
                };

                format!("{}: {} Run{}", score.source, score.runs, plural)
            })
            .join("\n");

        if scores.len() > 1 {
            score_ledger = format!("{}\n{} = {}", score_ledger, score_expression, score_total);
        }

        game.get("scoreLedger").set(score_ledger)?;
        game.get("scoreUpdate").set(format!("{} Runs scored!", score_total))?;

        let top_of_inning = game.get("topOfInning").as_bool()?;
        game.get(&prefixed("Score", top_of_inning)).map_int(|runs| runs + score_total)?;
        game.get(&inning_prefixed("InningScore", top_of_inning)).map_int(|runs| runs + score_total)?;
        game.get("halfInningScore").map_int(|runs| runs + score_total)?;
    }

    Ok(())
}

fn apply_half_inning(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying HalfInning event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let inning = game.get("inning").as_int()?;
    let top_of_inning = game.get("topOfInning").as_bool()?;

    let new_inning = if top_of_inning { inning } else { inning + 1 };
    let new_top_of_inning = !top_of_inning;

    let batting_team_id = game.get(&prefixed("Team", new_top_of_inning)).as_uuid()?;
    let batting_team_name = data.get_team(&batting_team_id).get("fullName").as_string()?;

    let top_or_bottom = if new_top_of_inning { "Top" } else { "Bottom" };
    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} of {}, {} batting.\n", top_or_bottom, new_inning + 1, batting_team_name);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;

    game.get("phase").set(6)?;
    game.get("topOfInning").set(new_top_of_inning)?;
    game.get("inning").set(new_inning)?;
    game.get("halfInningScore").set(0)?;

    // The first halfInning event re-sets the data that PlayBall clears
    if inning == -1 {
        let away_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), "awayTeam"))?;
        let away_pitcher = get_active_pitcher(&state, away_team_id, event.day > 0)?;

        let home_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), "homeTeam"))?;
        let home_pitcher = get_active_pitcher(&state, home_team_id, event.day > 0)?;

        for (pitcher, which) in [(home_pitcher, "home"), (away_pitcher, "away")] {
            game.get(&format!("{}Pitcher", which)).set(pitcher.pitcher_id)?;
            game.get(&format!("{}PitcherName", which)).set(pitcher.pitcher_name)?;
        }
    }

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}

fn apply_batter_up(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying BatterUp event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let top_of_inning = game.get("topOfInning").as_bool()?;
    let batting_team_id = game.get(&prefixed("Team", top_of_inning)).as_uuid()?;
    let batting_team = data.get_team(&batting_team_id);
    let batting_team_name = batting_team.get("nickname").as_string()?;
    let batter_count = 1 + game.get(&prefixed("TeamBatterCount", top_of_inning)).as_int()?;
    let (batter_id, batter_name) = get_next_batter(&data, &batting_team, batter_count)?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} batting for the {}.\n", batter_name, batting_team_name);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get(&prefixed("Batter", top_of_inning)).set(batter_id)?;
    game.get(&prefixed("BatterName", top_of_inning)).set(batter_name)?;
    game.get(&prefixed("TeamBatterCount", top_of_inning)).set(batter_count)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}

fn get_next_batter(data: &DataView, batting_team: &EntityView, batter_count: i64) -> IngestResult<(Uuid, String)> {
    let lineup_node = batting_team.get("lineup");
    // New scope introduced to avoid deadlock
    let batter_id = {
        let lineup = lineup_node.as_array()?;
        lineup.get(batter_count as usize % lineup.len()).unwrap().as_uuid()
            .map_err(|value| anyhow!("Expected lineup to have uuid values but it had {}", value))?
    };

    let player = data.get_player(&batter_id);
    let batter_name = player.get("name").as_string()?;

    Ok((batter_id, batter_name))
}


fn apply_strike(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Strike event for game {}", game_id))?;

    let balls = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls"))?;
    let strikes = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes"))?;

    let strike_type = parse_strike(&event.description)?;
    let strike_text = match strike_type {
        StrikeType::Swinging => { "swinging" }
        StrikeType::Looking => { "looking" }
    };

    log.debug(format!("Recording Strike, {} for game {}, count {}-{}", strike_text, game_id, balls, strikes))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("Strike, {}. {}-{}\n", strike_text, balls, strikes);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get("atBatStrikes").set(strikes)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Ball event for game {}", game_id))?;

    let balls = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls"))?;
    let strikes = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes"))?;

    log.debug(format!("Recording Ball for game {}, count {}-{}", game_id, balls, strikes))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("Ball. {}-{}\n", balls, strikes);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get("atBatBalls").set(balls)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_foul_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying FoulBall event for game {}", game_id))?;

    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning"))?;
    let max_strikes = state.int_at(&bs::json_path!("game", game_id.clone(), prefixed("Strikes", top_of_inning)))?;

    let balls = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls"))?;
    let mut strikes = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes"))?;

    if strikes + 1 < max_strikes {
        strikes += 1;
    }

    log.debug(format!("Recording FoulBall for game {}, count {}-{}", game_id, balls, strikes))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("Foul Ball. {}-{}\n", balls, strikes);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get("atBatStrikes").set(strikes)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_ground_out(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying GroundOut event for game {}", game_id))?;

    // Look. I accidentally wrote the parsing logic to tell ground outs and flyouts apart before
    // realizing that they're separate event types, so I'm just using it now.
    apply_fielding_out(state, log, event, game_id)
}

pub struct TopInningEnd {
    game_id: Uuid,
    play_count: i64,
    at_time: DateTime<Utc>,
}

impl IngestItem for TopInningEnd {
    fn date(&self) -> DateTime<Utc> {
        self.at_time
    }

    fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestApplyResult {
        log.info(format!("Applying generated TopInningEnd event for game {}", self.game_id))?;

        let data = DataView::new(state.data.clone(),
                                 bs::Event::TimedChange(self.at_time));
        let game = data.get_game(&self.game_id);

        game_update(&game, &[], "", self.play_count, &[])?;
        game.get("phase").set(2)?;

        let (new_data, caused_by) = data.into_inner();
        Ok((state.successor(caused_by, new_data), Vec::new()))
    }
}


fn apply_fielding_out(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent, game_id: &Uuid) -> IngestApplyResult {
    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let top_of_inning = game.get("topOfInning").as_bool()?;
    let batter_id = game.get(&prefixed("Batter", top_of_inning)).as_uuid()?;
    let batter_name = game.get(&prefixed("BatterName", top_of_inning)).as_string()?;

    let (scoring_runners, other_events) = separate_scoring_events(&event.metadata.siblings)?;

    let out = match other_events.len() {
        1 => parse_simple_out(&batter_name, &other_events[0].description),
        2 => parse_complex_out(&batter_name, &other_events[0].description, &other_events[1].description),
        more => Err(anyhow!("Unexpected fielding out with {} non-score siblings", more))
    }?;

    let mut message = match out {
        FieldingOut::GroundOut(fielder_name) => {
            format!("{} hit a ground out to {}.\n", batter_name, fielder_name)
        }
        FieldingOut::Flyout(fielder_name) => {
            format!("{} hit a flyout to {}.\n", batter_name, fielder_name)
        }
        FieldingOut::FieldersChoice(runner_name, base) => {
            format!("{} out at {} base.\n{} reaches on fielder's choice.\n", runner_name, base.name(), batter_name)
        }
        FieldingOut::DoublePlay => {
            format!("{} hit into a double play!\n", batter_name)
        }
    };

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let batter = data.get_player(&batter_id);

    let scores: Vec<_> = scoring_runners.iter()
        .map(|&runner_id| {
            let score = score_runner(&data, &game, runner_id, "Sacrifice")?;

            message = format!("{}{} advances on the sacrifice.\n", message, score.player_name);

            Ok::<_, IngestError>(score)
        })
        .try_collect()?;

    let scoring_team = score_team(&data, &game, top_of_inning, &mut message, scoring_runners)?;

    let outs_added = if let FieldingOut::DoublePlay = out { 2 } else { 1 };
    let num_outs = game.get("halfInningOuts").as_int()? + outs_added;
    let internal_events = apply_out(log, &game, &batter, event, message, play, &scores, top_of_inning, num_outs)?;

    if let Some(team_id) = scoring_team {
        game.get("lastUpdateFull").get(event.metadata.siblings.len() - 1).get("teamTags").push(team_id)?;
    }

    if let FieldingOut::FieldersChoice(runner_name_parsed, out_at_base) = out {
        let runner_idx = get_baserunner_with_name(&game, runner_name_parsed, out_at_base)?;
        remove_base_runner(&game, runner_idx)?;
        // The order is very particular
        advance_runners(&game, 0)?;
        push_base_runner(&game, batter_id, batter_name, Base::First)?;
    } else {
        advance_runners(&game, 0)?;
    }

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), internal_events))
}

fn separate_scoring_events(siblings: &Vec<EventuallyEvent>) -> IngestResult<(Vec<&Uuid>, Vec<&EventuallyEvent>)> {
    // The first event is never a scoring event, and it mixes up the rest of the logic because the
    // "hit" event type is reused
    let (first, rest) = siblings.split_first()
        .ok_or(anyhow!("Event had no siblings (including itself)"))?;
    let mut scores = Vec::new();
    let mut others = vec![first];


    for event in rest {
        if event.r#type == EventType::Hit {
            scores.push(get_one_id(&event.player_tags, "playerTags")?);
        } else if event.r#type != EventType::RunsScored {
            others.push(event);
        }
    }

    Ok((scores, others))
}

fn remove_base_runner(game: &EntityView<'_>, runner_idx: usize) -> IngestResult<()> {
    game.get("baseRunners").remove(runner_idx)?;
    game.get("baseRunnerNames").remove(runner_idx)?;
    game.get("baseRunnerMods").remove(runner_idx)?;
    game.get("basesOccupied").remove(runner_idx)?;

    game.get("baserunnerCount").map_int(|count| count - 1)?;

    Ok(())
}

struct Score {
    player_name: String,
    source: &'static str,
    runs: i64, // falsehoods
}


fn apply_hit(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Hit event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = game.get("topOfInning").as_bool()?;
    let batter_id = game.get(&prefixed("Batter", top_of_inning)).as_uuid()?;
    let batter_name = game.get(&prefixed("BatterName", top_of_inning)).as_string()?;

    if player_id != &batter_id {
        return Err(anyhow!("Batter id from state ({}) didn't match batter id from event ({})", batter_id, player_id));
    }
    let hit_type = parse_hit(&batter_name, &event.description)?;
    let hit_text = match hit_type {
        Base::First => { "Single" }
        Base::Second => { "Double" }
        Base::Third => { "Triple" }
        Base::Fourth => { "Quadruple" }
    };

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let mut message = format!("{} hits a {}!\n", batter_name, hit_text);

    let (scoring_runners, _) = separate_scoring_events(&event.metadata.siblings)?;

    let scores: Vec<_> = scoring_runners.iter()
        .map(|&runner_id| {
            let score = score_runner(&data, &game, runner_id, "Base Hit")?;

            message = format!("{}{} scores!\n", message, score.player_name);

            Ok::<_, IngestError>(score)
        })
        .try_collect()?;

    let scoring_team = score_team(&data, &game, top_of_inning, &mut message, scoring_runners)?;

    game_update(&game, &event.metadata.siblings, message, play, &scores)?;
    if let Some(team_id) = scoring_team {
        game.get("lastUpdateFull").get(event.metadata.siblings.len() - 1).get("teamTags").push(team_id)?;
    }
    advance_runners(&game, hit_type as i64 + 1)?;
    push_base_runner(&game, batter_id, batter_name, hit_type)?;
    end_at_bat(&game, top_of_inning)?;
    let player = data.get_player(player_id);
    player.get("consecutiveHits").map_int(|hits| hits + 1)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}

fn score_team(data: &DataView, game: &EntityView, top_of_inning: bool, message: &mut String, scoring_runners: Vec<&Uuid>) -> IngestResult<Option<Uuid>> {
    if scoring_runners.len() > 0 {
        let team_id = game.get(&prefixed("Team", top_of_inning)).as_uuid()?;
        let team = data.get_team(&team_id);
        let team_nickname = team.get("nickname").as_string()?;
        *message = format!("{}The {} scored!\n", message, team_nickname);

        Ok(Some(team_id))
    } else {
        Ok(None)
    }
}

fn score_runner(data: &DataView, game: &EntityView, runner_id: &Uuid, source: &'static str) -> IngestResult<Score> {
    let runner = data.get_player(runner_id);
    let runner_name = runner.get("name").as_string()?;

    let score = Score {
        player_name: runner_name,
        source,
        runs: 1,
    };

    let runner_from_state = game.get("baseRunners").pop_front()?
        .ok_or_else(|| anyhow!("Failed to remove baseRunners item on scoring event"))?
        .as_uuid()
        .map_err(|value| anyhow!("Expected baseRunners to have uuid values but found {}", value))?;
    if runner_from_state != *runner_id {
        return Err(anyhow!("Got a scoring event for {} but {} was first in the list", runner_id, runner_from_state));
    }
    game.get("baseRunnerNames").pop_front()?
        .ok_or_else(|| anyhow!("Failed to remove baseRunnerNames item on scoring event"))?;
    game.get("baseRunnerMods").pop_front()?
        .ok_or_else(|| anyhow!("Failed to remove baseRunnerMods item on scoring event"))?;
    // TODO Use the fact that they scored to narrow down IntRange possibilities for basesOccupied
    game.get("basesOccupied").pop_front()?
        .ok_or_else(|| anyhow!("Failed to remove basesOccupied item on scoring event"))?;
    game.get("baserunnerCount").map_int(|count| count - 1)?;

    Ok(score)
}

fn advance_runners(game: &EntityView<'_>, advance_at_least: i64) -> IngestResult<()> {
    for base in game.get("basesOccupied").as_array_mut()?.iter_mut() {
        let new_range = if let Ok(current_base) = base.as_int() {
            let min_base = current_base + advance_at_least;
            // I think you can only advance 1 extra base, and you can only advance to third
            // (otherwise it's a run; I'll deal with fifth base later)
            let max_base = min(min_base + 1, 2);

            Ok(bs::PrimitiveValue::IntRange(min_base, max_base))
        } else if let Ok((lower, upper)) = base.as_int_range() {
            let min_base = lower + advance_at_least;
            let max_base = min(upper + advance_at_least + 1, 2);

            Ok(bs::PrimitiveValue::IntRange(min_base, max_base))
        } else {
            Err(anyhow!("Expected basesOccupied to have int or int range values but it had {}", base.to_string()))
        }?;

        *base = base.successor(new_range, game.caused_by());
    }

    Ok(())
}

fn push_base_runner(game: &EntityView<'_>, runner_id: Uuid, runner_name: String, to_base: Base) -> IngestResult<()> {
    game.get("baseRunners").push(runner_id)?;
    game.get("baseRunnerNames").push(runner_name)?;
    game.get("baseRunnerMods").push("")?;
    game.get("basesOccupied").push(to_base as i64)?;

    game.get("baserunnerCount").map_int(|count| count + 1)?;

    let mut last_occupied_base = None;
    for base in game.get("basesOccupied").as_array_mut()?.iter_mut().rev() {
        if let Ok(base_num) = base.as_int() {
            if let Some(last_occupied_base_num) = last_occupied_base {
                if base_num <= last_occupied_base_num {
                    let last_occupied_base_num = base_num + 1;

                    *base = base.successor(last_occupied_base_num.into(), game.caused_by());
                    last_occupied_base = Some(last_occupied_base_num)
                } else {
                    last_occupied_base = Some(base_num);
                }
            } else {
                last_occupied_base = Some(base_num);
            }
        } else if let Ok((min_base, max_base)) = base.as_int_range() {
            if let Some(last_occupied_base_num) = last_occupied_base {
                if min_base <= last_occupied_base_num {
                    let last_occupied_base_num = min_base + 1;

                    if last_occupied_base_num == max_base {
                        // Then this has collapsed the possibilities
                        *base = base.successor(last_occupied_base_num.into(), game.caused_by())
                    } else {
                        // Then this has just narrowed down the range
                        *base = base.successor(bs::PrimitiveValue::IntRange(last_occupied_base_num, max_base), game.caused_by())
                    }
                    last_occupied_base = Some(last_occupied_base_num)
                } else {
                    last_occupied_base = Some(min_base);
                }
            } else {
                last_occupied_base = Some(min_base);
            }
        } else {
            return Err(anyhow!("Expected basesOccupied to have int values but it had {}", base.to_string()));
        }
    }

    Ok(())
}


fn apply_strikeout(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Strikeout event for game {}", game_id))?;

    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning"))?;
    let batter_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning)))?;

    if player_id != &batter_id {
        return Err(anyhow!("Batter id from state ({}) didn't match batter id from event ({})", batter_id, player_id));
    }

    let batter_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning)))?;
    let strike_type = parse_strikeout(&batter_name, &event.description)?;

    let strike_text = match strike_type {
        StrikeType::Swinging => { "swinging" }
        StrikeType::Looking => { "looking" }
    };

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);
    let batter = data.get_player(&batter_id);

    let num_outs = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "halfInningOuts"))?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} strikes out {}.\n", batter_name, strike_text);
    let internal_events = apply_out(log, &game, &batter, event, message, play, &[], top_of_inning, num_outs)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), internal_events))
}

fn end_at_bat(game: &EntityView, top_of_inning: bool) -> IngestResult<()> {
    game.get(&prefixed("Batter", top_of_inning)).set(bs::PrimitiveValue::Null)?;
    game.get(&prefixed("BatterName", top_of_inning)).set("")?;
    game.get("atBatBalls").set(0)?;
    game.get("atBatStrikes").set(0)?;

    Ok(())
}

fn apply_out<'a, T: Into<String>>(
    log: &'a IngestLogger<'a>,
    game: &'a EntityView,
    batter: &'a EntityView,
    event: &'a EventuallyEvent,
    message: T,
    play: i64,
    scores: &[Score],
    top_of_inning: bool,
    num_outs: i64,
) -> IngestResult<Vec<Box<dyn IngestItem>>> {
    log.info(format!("Observed out by {}. Zeroing consecutiveHits", batter.get("name").as_string()?))?;

    game_update(game, &event.metadata.siblings, message, play, scores)?;
    end_at_bat(game, top_of_inning)?;
    batter.get("consecutiveHits").set(0)?;

    let end_of_half_inning = num_outs == 3;
    if end_of_half_inning {
        game.get("halfInningOuts").set(0)?;
        game.get("phase").set(3)?;
        clear_bases(game)?;

        // Reset both top and bottom inning scored only when the bottom half ends
        if !top_of_inning {
            game.get("topInningScore").set(0)?;
            game.get("bottomInningScore").set(0)?;
            game.get("halfInningScore").set(0)?;
        }
    } else {
        game.get("halfInningOuts").set(num_outs)?;
    }

    let mut events: Vec<Box<dyn IngestItem>> = Vec::new();
    if top_of_inning && end_of_half_inning {
        events.push(Box::new(TopInningEnd {
            game_id: game.entity_id.clone(),
            play_count: play + 1,
            at_time: event.created + Duration::seconds(5),
        }))
    }
    Ok(events)
}

fn clear_bases(game: &EntityView) -> IngestResult<()> {
    game.get("baseRunners").overwrite(json!([]))?;
    game.get("baseRunnerNames").overwrite(json!([]))?;
    game.get("baseRunnerMods").overwrite(json!([]))?;
    game.get("basesOccupied").overwrite(json!([]))?;
    game.get("baserunnerCount").set(0)?;

    Ok(())
}

fn get_one_id<'a>(tags: &'a Vec<Uuid>, field_name: &'static str) -> IngestResult<&'a Uuid> {
    if tags.len() != 1 {
        return Err(anyhow!("Expected exactly one element in {} but found {}", field_name, tags.len()));
    }

    tags.get(0)
        .ok_or_else(|| anyhow!("Expected exactly one element in {} but found none", field_name))
}


fn apply_fly_out(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying FlyOut event for game {}", game_id))?;

    // Look. I accidentally wrote the parsing logic to tell ground outs and flyouts apart before
    // realizing that they're separate event types, so I'm just using it now.
    apply_fielding_out(state, log, event, game_id)
}


fn apply_stolen_base(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying StolenBase event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let thief_uuid = get_one_id(&event.player_tags, "playerTags")?;
    let thief = data.get_player(thief_uuid);
    let thief_name = thief.get("name").as_string()?;

    let which_base = parse_stolen_base(&thief_name, &event.description)?;

    let baserunner_index = get_baserunner_with_uuid(&game, thief_uuid, which_base)?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} steals {} base!\n", thief_name, which_base.name());

    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    let bases_occupied = game.get("basesOccupied");
    bases_occupied.get(baserunner_index).map_int(|base| base + 1)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}

fn get_baserunner_with_uuid(game: &EntityView<'_>, expected_uuid: &Uuid, which_base: Base) -> Result<usize, anyhow::Error> {
    let baserunner_uuids: Vec<_> = game.get("baseRunners").as_array()?.iter()
        .map(|uuid_node| {
            uuid_node.as_uuid()
                .map_err(|val| anyhow!("Expected uuids in baseRunners array but found {}", val))
        })
        .try_collect()?;

    get_baserunner_with_property(game, expected_uuid, which_base, baserunner_uuids)?
        .ok_or_else(|| anyhow!("Couldn't find baserunner with uuid {} on {} base", expected_uuid, which_base.name()))
}

fn get_baserunner_with_name(game: &EntityView<'_>, expected_name: &str, base_plus_one: Base) -> Result<usize, anyhow::Error> {
    let baserunner_names: Vec<_> = game.get("baseRunnerNames").as_array()?.iter()
        .map(|uuid_node| {
            uuid_node.as_string()
                .map_err(|val| anyhow!("Expected string in baseRunnerNames array but found {}", val))
        })
        .try_collect()?;

    get_baserunner_with_property(game, expected_name, base_plus_one, baserunner_names)?
        .ok_or_else(|| anyhow!("Couldn't find baserunner with name {} on {} base", expected_name, base_plus_one.name()))
}

fn get_baserunner_with_property<T: ?Sized, U: std::cmp::PartialEq<T>>(
    game: &EntityView<'_>, expected_property: &T, which_base: Base, baserunner_properties: Vec<U>,
) -> IngestResult<Option<usize>> {
    let baserunner_bases = get_bases_occupied(game)?;

    let possible_baserunner_indices: Vec<_> = Iterator::zip(baserunner_properties.into_iter(), baserunner_bases.into_iter())
        .enumerate()
        .filter_map(|(i, (name, base))| {
            if &name == expected_property && base + 1 == which_base as i64 {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if let Some((baserunner_index, )) = possible_baserunner_indices.into_iter().collect_tuple() {
        Ok(Some(baserunner_index))
    } else {
        Ok(None)
    }
}

fn get_bases_occupied(game: &EntityView<'_>) -> IngestResult<Vec<i64>> {
    let baserunner_bases: Vec<_> = game.get("basesOccupied").as_array()?.iter()
        .map(|base_node| {
            base_node.as_int()
                .map_err(|val| anyhow!("Expected ints in basesOccupied array but found {}", val))
        })
        .try_collect()?;

    Ok(baserunner_bases)
}


fn apply_walk(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Walk event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = game.get("topOfInning").as_bool()?;
    let batter_id = game.get(&prefixed("Batter", top_of_inning)).as_uuid()?;
    let batter_name = game.get(&prefixed("BatterName", top_of_inning)).as_string()?;

    if player_id != &batter_id {
        return Err(anyhow!("Batter id from state ({}) didn't match batter id from event ({})", batter_id, player_id));
    }

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} draws a walk.\n", batter_name);

    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    push_base_runner(&game, batter_id, batter_name, Base::First)?;
    end_at_bat(&game, top_of_inning)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_home_run(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying HomeRun event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning"))?;
    let batter_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning)))?;
    let batter_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning)))?;
    let team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Team", top_of_inning)))?;
    let team_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("TeamNickname", top_of_inning)))?;

    if player_id != &batter_id {
        return Err(anyhow!("Batter id from state ({}) didn't match batter id from event ({})", batter_id, player_id));
    }
    let num_runs = parse_home_run(&batter_name, &event.description)?;
    let home_run_text = match num_runs {
        1 => { "solo".into() }
        num => { format!("{}-run", num) }
    };

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} hits a {} home run!\nThe {} scored!\n", batter_name, home_run_text, team_name);
    let scoring_runners: Vec<_> = game.get("baseRunners").as_array()?.iter()
        .map(|node| {
            node.as_uuid()
                .map_err(|value| anyhow!("Expected baseRunners to have uuid values but found {}", value))
        })
        .try_collect()?;

    let scores: Vec<_> = scoring_runners.iter()
        .map(|runner_id| {
            score_runner(&data, &game, runner_id, "Home Run")
        })
        .chain(iter::once(Ok(Score {
            player_name: batter_name,
            source: "Home Run",
            runs: 1,
        })))
        .try_collect()?;

    game_update(&game, &event.metadata.siblings, message, play, &scores)?;
    end_at_bat(&game, top_of_inning)?;
    let player = data.get_player(&batter_id);
    player.get("consecutiveHits").map_int(|n| n + 1)?;
    game.get("lastUpdateFull").get(1).get("teamTags").push(team_id)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_storm_warning(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying StormWarning event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;

    game_update(&game, &event.metadata.siblings, "WINTER STORM WARNING\n", play, &[])?;
    game.get("gameStartPhase").set(11)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn format_blaseball_date(date: DateTime<Utc>) -> String {
    // For some godforsaken reason, blaseball dates strip trailing millisecond zeros
    let main_date = date.format("%Y-%m-%dT%H:%M:%S");
    let millis = date.timestamp_millis() % 1000;
    if millis == 0 {
        return format!("{}Z", main_date);
    }
    let millis_str = format!("{:0>3}", millis);
    let millis_trimmed = millis_str.trim_end_matches("0");
    let millis_final = if millis_trimmed.is_empty() { "0" } else { millis_trimmed };
    format!("{}.{}Z", main_date, millis_final)
}


fn apply_snowflakes(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Snowflakes event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let (snow_event, mod_events) = event.metadata.siblings.split_first()
        .ok_or(anyhow!("Snowflakes event is missing metadata.siblings"))?;

    let (num_snowflakes, modified_type) = parse_snowfall(&snow_event.description)?;
    let frozen_player_ids: Vec<_> = mod_events.iter()
        .map(|event| {
            get_one_id(&event.player_tags, "playerTags")
        })
        .try_collect()?;

    let frozen_messages: Vec<String> = frozen_player_ids.iter()
        .map(|&uuid| {
            let player = data.get_player(uuid);
            Ok::<String, anyhow::Error>(format!("\n{} was Frozen!", player.get("name").as_string()?))
        })
        .try_collect()?;

    let frozen_player_and_team_ids: Vec<_> = frozen_player_ids.into_iter()
        .map(|player_id| {
            let player = data.get_player(&player_id);
            let team_id = player.get("leagueTeamId").as_uuid()?;
            Ok::<_, anyhow::Error>((player_id, team_id))
        })
        .try_collect()?;

    let message = format!("{} Snowflakes {} the field!{}\n", num_snowflakes, modified_type, frozen_messages.join(""));
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get("gameStartPhase").set(20)?;
    game.get("state").get("snowfallEvents").map_int(|x| x + 1)?;

    frozen_player_and_team_ids.into_iter()
        .enumerate()
        .try_for_each(|(i, (player_id, team_id))| {
            let player = data.get_player(&player_id);
            game.get("lastUpdateFull").get(i + 1).get("teamTags").push(team_id)?;
            player.get("gameAttr").push("FROZEN")?;

            Ok::<_, bs::PathError>(())
        })?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_player_stat_reroll(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    log.debug(format!("Applying PlayerStatReroll event for player {}", player_id))?;


    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));

    let snow_attrs = [
        "baseThirst", "baserunningRating", "buoyancy", "coldness", "defenseRating", "divinity",
        "groundFriction", "hittingRating", "indulgence", "laserlikeness", "martyrdom", "moxie",
        "musclitude", "omniscience", "overpowerment", "patheticism", "pitchingRating",
        "ruthlessness", "shakespearianism",
    ];

    let player = data.get_player(player_id);
    for attr_name in snow_attrs {
        // +/-0.1 is a placeholder
        player.get(attr_name).map_float_range(|lower, upper|
            bs::PrimitiveValue::FloatRange(lower - 0.1, upper + 0.1))?;
    }

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_inning_end(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying InningEnd event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let inning = game.get("inning").as_int()?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;

    let message = format!("Inning {} is now an Outing.\n", inning + 1);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get("phase").set(2)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}


fn apply_batter_skipped(state: Arc<bs::BlaseballState>, log: &IngestLogger<'_>, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying BatterSkipped event for game {}", game_id))?;

    let data = DataView::new(state.data.clone(),
                             bs::Event::FeedEvent(event.id));
    let game = data.get_game(game_id);

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let player_id = get_one_id(&event.player_tags, "playerTags")?;

    let top_of_inning = game.get("topOfInning").as_bool()?;
    let player = data.get_player(player_id);
    let batter_name = player.get("name").as_string()?;
    let message = format!("{} is Frozen!\n", batter_name);
    game_update(&game, &event.metadata.siblings, message, play, &[])?;
    game.get(&prefixed("TeamBatterCount", top_of_inning)).map_int(|i| i + 1)?;

    let (new_data, caused_by) = data.into_inner();
    Ok((state.successor(caused_by, new_data), Vec::new()))
}