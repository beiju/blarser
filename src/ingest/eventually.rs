use std::cmp::min;
use std::future::Future;
use std::sync::Arc;
use anyhow::anyhow;
use rocket::async_trait;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde_json::{json, Value};
use uuid::Uuid;
use serde::Deserialize;

use crate::api::{eventually, EventuallyEvent, EventType, Weather};
use crate::blaseball_state as bs;
use crate::blaseball_state::{BlaseballState, ChangeType};
use crate::ingest::{IngestItem, BoxedIngestItem, IngestResult, IngestError};
use crate::ingest::error::IngestApplyResult;
use crate::ingest::log::IngestLogger;
use crate::ingest::text_parser::{FieldingOut, Base, StrikeType, parse_fielding_out, parse_hit, parse_home_run, parse_snowfall, parse_strike, parse_strikeout, parse_stolen_base};

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

    async fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestApplyResult {
        log.debug(format!("Applying Feed event {} from {}: \"{}\"", self.id, self.created, self.description)).await?;

        let result = match self.r#type {
            EventType::BigDeal => apply_big_deal(state, log, self).await,
            EventType::LetsGo => apply_lets_go(state, log, self).await,
            EventType::PlayBall => apply_play_ball(state, log, self).await,
            EventType::HalfInning => apply_half_inning(state, log, self).await,
            EventType::BatterUp => apply_batter_up(state, log, self).await,
            EventType::Strike => apply_strike(state, log, self).await,
            EventType::Ball => apply_ball(state, log, self).await,
            EventType::FoulBall => apply_foul_ball(state, log, self).await,
            EventType::GroundOut => apply_ground_out(state, log, self).await,
            EventType::Hit => apply_hit(state, log, self).await,
            EventType::Strikeout => apply_strikeout(state, log, self).await,
            EventType::FlyOut => apply_fly_out(state, log, self).await,
            EventType::StolenBase => apply_stolen_base(state, log, self).await,
            EventType::Walk => apply_walk(state, log, self).await,
            EventType::HomeRun => apply_home_run(state, log, self).await,
            EventType::StormWarning => apply_storm_warning(state, log, self).await,
            EventType::Snowflakes => apply_snowflakes(state, log, self).await,
            EventType::PlayerStatReroll => apply_player_stat_reroll(state, log, self).await,
            _ => todo!()
        };

        log.increment_parsed_events().await?;

        result
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlayMetadata {
    pub play: i64,
}


async fn apply_big_deal(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Ignoring BigDeal event".to_string()).await?;
    Ok(state)
}


async fn apply_lets_go(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying LetsGo event for game {}", game_id)).await?;

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct LetsGoMetadata {
        pub home: Uuid,
        pub away: Uuid,
        pub stadium: Option<Uuid>,
        pub weather: Weather,
    }

    let metadata: LetsGoMetadata = serde_json::from_value(event.metadata.other.clone())?;
    let away_pitcher = get_active_pitcher(&state, metadata.home, event.day > 0).await?;
    let home_pitcher = get_active_pitcher(&state, metadata.away, event.day > 0).await?;

    let diff = [
        // Team object changes
        bs::Patch {
            path: bs::json_path!("team", metadata.home, "rotationSlot"),
            change: bs::ChangeType::Set(home_pitcher.rotation_slot.into()),
        },
        bs::Patch {
            path: bs::json_path!("team", metadata.away, "rotationSlot"),
            change: bs::ChangeType::Set(away_pitcher.rotation_slot.into()),
        },

        // Game object changes
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "gameStart"),
            change: bs::ChangeType::Set(true.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "gameStartPhase"),
            change: bs::ChangeType::Set((-1).into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "homeTeamBatterCount"),
            change: bs::ChangeType::Set((-1).into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "awayTeamBatterCount"),
            change: bs::ChangeType::Set((-1).into()),
        },
    ].into_iter();

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}

struct ActivePitcher {
    rotation_slot: i64,
    pitcher_id: Uuid,
    pitcher_name: String,
}

async fn get_active_pitcher(state: &Arc<bs::BlaseballState>, team_id: Uuid, advance: bool) -> Result<ActivePitcher, bs::PathError> {
    let rotation = state.array_at(&bs::json_path!("team", team_id, "rotation")).await?;
    let rotation_slot = state.int_at(&bs::json_path!("team", team_id, "rotationSlot")).await?;
    let rotation_slot = if advance {
        (rotation_slot + 1) % rotation.len() as i64
    } else {
        rotation_slot
    };

    let pitcher_id = rotation.get(rotation_slot as usize)
        .expect("rotation_slot should always be valid here");

    let pitcher_id = pitcher_id.as_uuid().await
        .map_err(|value| bs::PathError::UnexpectedType {
            path: bs::json_path!("team", team_id, "rotation", rotation_slot as usize),
            expected_type: "uuid",
            value,
        })?;

    let pitcher_name = state.string_at(&bs::json_path!("player", pitcher_id, "name")).await?;

    Ok(ActivePitcher { rotation_slot, pitcher_id, pitcher_name })
}

async fn apply_play_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying PlayBall event for game {}", game_id)).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let diff = common_patches(&event.metadata.siblings, game_id, "Play ball!".into(), play, true)
        .chain(play_ball_team_specific_diffs(game_id, "away"))
        .chain(play_ball_team_specific_diffs(game_id, "home"))
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "gameStartPhase"),
                change: bs::ChangeType::Set(20.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "inning"),
                change: bs::ChangeType::Set((-1).into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "phase"),
                change: bs::ChangeType::Set(2.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "topOfInning"),
                change: bs::ChangeType::Set(false.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}

fn prefixed(text: &'static str, top_of_inning: bool) -> String {
    let home_or_away = if top_of_inning { "away" } else { "home" };
    format!("{}{}", home_or_away, text)
}

fn inning_prefixed(text: &'static str, top_of_inning: bool) -> String {
    let home_or_away = if top_of_inning { "top" } else { "bottom" };
    format!("{}{}", home_or_away, text)
}

fn play_ball_team_specific_diffs(game_id: &Uuid, which: &'static str) -> impl Iterator<Item=bs::Patch> {
    [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Pitcher", which)),
            change: bs::ChangeType::Set(bs::PrimitiveValue::Null),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}PitcherName", which)),
            change: bs::ChangeType::Set("".into()),
        },
    ].into_iter()
}

fn common_patches(events: &Vec<EventuallyEvent>, game_id: &Uuid, message: String, play: i32, clear_runs: bool) -> impl Iterator<Item=bs::Patch> {
    let diff = [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "lastUpdate"),
            change: bs::ChangeType::Set(format!("{}\n", message).into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "playCount"),
            // play and playCount are out of sync by exactly 1
            change: bs::ChangeType::Set((play + 1).into()),
        },
        // lastUpdateFull is not logically connected to the previous one. Re-set it each time
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "lastUpdateFull"),
            change: bs::ChangeType::Overwrite(Value::Array(events.iter()
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
                .collect())),
        },
    ].into_iter();

    if clear_runs {
        diff.chain(vec![
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "scoreLedger"),
                change: bs::ChangeType::Set("".into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "scoreUpdate"),
                change: bs::ChangeType::Set("".into()),
            },
        ])
    } else {
        diff.chain(vec![])
    }
}

async fn apply_half_inning(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying HalfInning event for game {}", game_id)).await?;

    let inning = state.int_at(&bs::json_path!("game", game_id.clone(), "inning")).await?;
    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;

    let new_inning = if top_of_inning { inning } else { inning + 1 };
    let new_top_of_inning = !top_of_inning;

    let batting_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Team", new_top_of_inning))).await?;
    let batting_team_name = state.string_at(&bs::json_path!("team", batting_team_id, "fullName")).await?;

    let top_or_bottom = if new_top_of_inning { "Top" } else { "Bottom" };
    let message = format!("{} of {}, {} batting.", top_or_bottom, new_inning + 1, batting_team_name);
    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "phase"),
                change: bs::ChangeType::Set((6).into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "topOfInning"),
                change: bs::ChangeType::Set(new_top_of_inning.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "inning"),
                change: bs::ChangeType::Set(new_inning.into()),
            },
        ]);

    // The first halfInning event re-sets the data that PlayBall clears
    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    if inning == -1 {
        let away_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), "awayTeam")).await?;
        let away_pitcher = get_active_pitcher(&state, away_team_id, event.day > 0).await?;

        let home_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), "homeTeam")).await?;
        let home_pitcher = get_active_pitcher(&state, home_team_id, event.day > 0).await?;

        let diff = diff.into_iter()
            .chain(half_inning_team_specific_diffs(game_id, away_pitcher, "away"))
            .chain(half_inning_team_specific_diffs(game_id, home_pitcher, "home"));

        state.successor(caused_by, diff).await
    } else {
        state.successor(caused_by, diff).await
    }
}

fn half_inning_team_specific_diffs(game_id: &Uuid, active_pitcher: ActivePitcher, which: &'static str) -> impl Iterator<Item=bs::Patch> {
    [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Pitcher", which)),
            change: bs::ChangeType::Set(active_pitcher.pitcher_id.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}PitcherName", which)),
            change: bs::ChangeType::Set(active_pitcher.pitcher_name.into()),
        },
    ].into_iter()
}


async fn apply_batter_up(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying BatterUp event for game {}", game_id)).await?;

    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let batting_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Team", top_of_inning))).await?;
    let batting_team_name = state.string_at(&bs::json_path!("team", batting_team_id, "nickname")).await?;
    let batter_count = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), prefixed("TeamBatterCount", top_of_inning))).await?;
    let batter_id = state.uuid_at(&bs::json_path!("team", batting_team_id, "lineup", batter_count as usize)).await?;
    let batter_name = state.string_at(&bs::json_path!("player", batter_id, "name")).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} batting for the {}.", batter_name, batting_team_name);
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning)),
                change: bs::ChangeType::Set(batter_id.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning)),
                change: bs::ChangeType::Set(batter_name.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), prefixed("TeamBatterCount", top_of_inning)),
                change: bs::ChangeType::Set(batter_count.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


async fn apply_strike(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Strike event for game {}", game_id)).await?;

    let balls = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls")).await?;
    let strikes = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes")).await?;

    let strike_type = parse_strike(&event.description)?;
    let strike_text = match strike_type {
        StrikeType::Swinging => { "swinging" }
        StrikeType::Looking => { "looking" }
    };

    log.debug(format!("Recording Strike, {} for game {}, count {}-{}", strike_text, game_id, balls, strikes)).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("Strike, {}. {}-{}", strike_text, balls, strikes);
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "atBatStrikes"),
                change: bs::ChangeType::Set(strikes.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


async fn apply_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Ball event for game {}", game_id)).await?;

    // let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    // let max_balls = state.int_at(&bs::json_path!("game", game_id.clone(), prefixed("Balls", top_of_inning))).await?;

    let balls = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls")).await?;
    let strikes = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes")).await?;

    log.debug(format!("Recording Ball for game {}, count {}-{}", game_id, balls, strikes)).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("Ball. {}-{}", balls, strikes);
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "atBatBalls"),
                change: bs::ChangeType::Set(balls.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


async fn apply_foul_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying FoulBall event for game {}", game_id)).await?;

    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let max_strikes = state.int_at(&bs::json_path!("game", game_id.clone(), prefixed("Strikes", top_of_inning))).await?;

    let balls = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls")).await?;
    let mut strikes = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes")).await?;

    if strikes != max_strikes {
        strikes += 1;
    }

    log.debug(format!("Recording FoulBall for game {}, count {}-{}", game_id, balls, strikes)).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("Foul Ball. {}-{}", balls, strikes);
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "atBatStrikes"),
                change: bs::ChangeType::Set(strikes.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


async fn apply_ground_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying GroundOut event for game {}", game_id)).await?;

    // Look. I accidentally wrote the parsing logic to tell ground outs and flyouts apart before
    // realizing that they're separate event types, so I'm just using it now.
    apply_fielding_out(state, log, event, game_id).await
}

async fn apply_fielding_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent, game_id: &Uuid) -> IngestApplyResult {
    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));

    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let batter_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning))).await?;
    let batter_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning))).await?;
    let num_outs = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "halfInningOuts")).await?;
    let out = parse_fielding_out(&batter_name, &event.description)?;

    let message = match out {
        FieldingOut::GroundOut(fielder_name) => {
            format!("{} hit a ground out to {}.", batter_name, fielder_name)
        },
        FieldingOut::Flyout(fielder_name) => {
            format!("{} hit a flyout to {}.", batter_name, fielder_name)
        },
        FieldingOut::FieldersChoice(runner_name, base) => {
            format!("{} out at {} base.{} reaches on fielder's choice.", runner_name, base.name(), batter_name)
        }
    };

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let diff = apply_out(log, &batter_id, event, game_id, message, play, top_of_inning, num_outs == 3).await?;

    state.successor(caused_by, diff).await
}


async fn apply_hit(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Hit event for game {}", game_id)).await?;

    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let batter_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning))).await?;
    let batter_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning))).await?;

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
    let message = format!("{} hits a {}!", batter_name, hit_text);
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain(push_base_runner(&state, game_id.clone(), batter_id, batter_name, hit_type as i64).await?)
        .chain(end_at_bat(game_id, top_of_inning))
        .chain([
            bs::Patch {
                path: bs::json_path!("player", player_id.clone(), "consecutiveHits"),
                change: bs::ChangeType::AddInt(1),
            },
        ]);

    let diff_vec: Vec<_> = diff.collect();
    for patch in &diff_vec {
        println!("Diff: {}", patch.description(&state).await?)
    }

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff_vec.into_iter()).await
}

async fn push_base_runner(state: &bs::BlaseballState, game_id: Uuid, runner_id: Uuid, runner_name: String, to_base: i64) -> IngestResult<impl Iterator<Item=bs::Patch>> {
    let existing_runners = state.array_at(&bs::json_path!("game", game_id.clone(), "basesOccupied")).await?;
    let existing_runners = stream::iter(existing_runners)
        .then(|current_base| async move {
            current_base.as_int().await
                .map_err(|value| anyhow!("Expected basesOccupied to be an array of ints, but found {}", value))
        })
        .try_collect::<Vec<_>>().await?;

    let it = existing_runners.into_iter()
        .enumerate()
        // Advance current runners
        .map(move |(i, current_base)| {
            let min_base = current_base + to_base + 1;
            // I think you can only advance 1 extra base, and you can only advance to third
            // (otherwise it's a run; I'll deal with fifth base later)
            let max_base = min(min_base + 1, 2);
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "basesOccupied", i),
                change: ChangeType::Set(bs::PrimitiveValue::IntRange(min_base, max_base)),
            }
        })
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "basesOccupied"),
                change: bs::ChangeType::Push(to_base.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "baseRunners"),
                change: bs::ChangeType::Push(runner_id.to_string().into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "baseRunnerNames"),
                change: bs::ChangeType::Push(runner_name.into()),
            },
            // Will implement this properly whenever it becomes relevant
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "baseRunnerMods"),
                change: bs::ChangeType::Push("".into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "baserunnerCount"),
                change: bs::ChangeType::AddInt(1),
            },
        ]);

    Ok(it)
}


async fn apply_strikeout(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Strikeout event for game {}", game_id)).await?;

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let batter_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning))).await?;

    if player_id != &batter_id {
        return Err(anyhow!("Batter id from state ({}) didn't match batter id from event ({})", batter_id, player_id));
    }

    let batter_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning))).await?;
    let num_outs = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "halfInningOuts")).await?;
    let strike_type = parse_strikeout(&batter_name, &event.description)?;

    let strike_text = match strike_type {
        StrikeType::Swinging => { "swinging" }
        StrikeType::Looking => { "looking" }
    };

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} strikes out {}.", batter_name, strike_text);
    let diff = apply_out(log, player_id, event, game_id, message, play, top_of_inning, num_outs == 3).await?;

    state.successor(caused_by, diff).await
}

fn end_at_bat(game_id: &Uuid, top_of_inning: bool) -> impl Iterator<Item=bs::Patch> {
    [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning)),
            change: bs::ChangeType::Set(bs::PrimitiveValue::Null),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning)),
            change: bs::ChangeType::Set("".into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "atBatBalls"),
            change: bs::ChangeType::Set(0.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "atBatStrikes"),
            change: bs::ChangeType::Set(0.into()),
        },
    ].into_iter()
}

fn score_runs(game_id: &Uuid, top_of_inning: bool, num_runs: i64, source_name: &str) -> impl Iterator<Item=bs::Patch> {
    let runs_plural = match num_runs {
        1 => "",
        _ => "s",
    };
    [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), prefixed("Score", top_of_inning)),
            change: bs::ChangeType::AddInt(num_runs),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "halfInningScore"),
            change: bs::ChangeType::AddInt(num_runs),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), inning_prefixed("InningScore", top_of_inning)),
            change: bs::ChangeType::AddInt(num_runs),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "scoreLedger"),
            change: bs::ChangeType::Set(format!("{}: {} Run{}", source_name, num_runs, runs_plural).into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), "scoreUpdate"),
            change: bs::ChangeType::Set(format!("{} Runs scored!", num_runs).into()),
        },
    ].into_iter()
}

fn apply_out<'a>(
    log: &'a IngestLogger,
    player_id: &'a Uuid,
    event: &'a EventuallyEvent,
    game_id: &'a Uuid,
    message: String,
    play: i32,
    top_of_inning: bool,
    end_of_inning: bool,
) -> impl Future<Output=Result<impl Iterator<Item=bs::Patch>, IngestError>> + 'a {
    async move {
        log.info(format!("Observed out by {}. Zeroing consecutiveHits", player_id)).await?;

        let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
            .chain(end_at_bat(game_id, top_of_inning))
            .chain([
                bs::Patch {
                    path: bs::json_path!("player", player_id.clone(), "consecutiveHits"),
                    change: bs::ChangeType::Set(0.into()),
                },
            ]);

        let diff = if end_of_inning {
            diff.chain(vec![
                bs::Patch {
                    path: bs::json_path!("game", game_id.clone(), "halfInningOuts"),
                    change: bs::ChangeType::Set(0.into()),
                },
                bs::Patch {
                    path: bs::json_path!("game", game_id.clone(), "phase"),
                    change: bs::ChangeType::Set(3.into()),
                },
            ])
        } else {
            diff.chain(vec![
                bs::Patch {
                    path: bs::json_path!("game", game_id.clone(), "halfInningOuts"),
                    change: bs::ChangeType::AddInt(1),
                },
            ])
        };

        Ok(diff)
    }
}

fn get_one_id<'a>(tags: &'a Vec<Uuid>, field_name: &'static str) -> IngestResult<&'a Uuid> {
    if tags.len() != 1 {
        return Err(anyhow!("Expected exactly one element in {} but found {}", field_name, tags.len()));
    }

    tags.get(0)
        .ok_or_else(|| anyhow!("Expected exactly one element in {} but found none", field_name))
}


async fn apply_fly_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying FlyOut event for game {}", game_id)).await?;

    // Look. I accidentally wrote the parsing logic to tell ground outs and flyouts apart before
    // realizing that they're separate event types, so I'm just using it now.
    apply_fielding_out(state, log, event, game_id).await
}


async fn apply_stolen_base(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying StolenBase event for game {}", game_id)).await?;

    let thief_uuid = get_one_id(&event.player_tags, "playerTags")?;
    let thief_name = state.string_at(&bs::json_path!("player", thief_uuid.clone(), "name")).await?;

    let which_base = parse_stolen_base(&thief_name, &event.description)?;

    let baserunner_index = get_baserunner(&state, game_id, thief_uuid, which_base).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} steals {} base!", thief_name, which_base.name());
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "basesOccupied", baserunner_index),
                change: bs::ChangeType::AddInt(1),
            },
        ]);

    let diff_vec: Vec<_> = diff.collect();
    for patch in &diff_vec {
        println!("Diff: {}", patch.description(&state).await?)
    }

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff_vec.into_iter()).await
}

async fn get_baserunner(state: &BlaseballState, game_id: &Uuid, thief_uuid: &Uuid, which_base: Base) -> Result<usize, anyhow::Error> {
    let baserunner_uuids: Vec<_> = stream::iter(state.array_at(&bs::json_path!("game", game_id.clone(), "baseRunners")).await?)
        .then(|uuid_node| async {
            uuid_node.as_uuid().await
                .map_err(|val| anyhow!("Expected uuids in baseRunners array but found {}", val))
        })
        .try_collect().await?;
    let baserunner_bases: Vec<_> = stream::iter(state.array_at(&bs::json_path!("game", game_id.clone(), "basesOccupied")).await?)
        .then(|base_node| async {
            base_node.as_int().await
                .map_err(|val| anyhow!("Expected ints in basesOccupied array but found {}", val))
        })
        .try_collect().await?;

    let possible_baserunner_indices: Vec<_> = Iterator::zip(baserunner_uuids.into_iter(), baserunner_bases.into_iter())
        .enumerate()
        .filter_map(|(i, (uuid, base))| {
            if &uuid == thief_uuid && base + 1 == which_base as i64 {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if let Some((baserunner_index, )) = possible_baserunner_indices.into_iter().collect_tuple() {
        Ok(baserunner_index)
    } else {
        Err(anyhow!("Couldn't determine which baserunner advanced on base steal"))
    }
}


async fn apply_walk(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Walk event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_home_run(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying HomeRun event for game {}", game_id)).await?;

    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let batter_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Batter", top_of_inning))).await?;
    let batter_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("BatterName", top_of_inning))).await?;
    let team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Team", top_of_inning))).await?;
    let team_name = state.string_at(&bs::json_path!("game", game_id.clone(), prefixed("TeamNickname", top_of_inning))).await?;

    if player_id != &batter_id {
        return Err(anyhow!("Batter id from state ({}) didn't match batter id from event ({})", batter_id, player_id));
    }
    let num_runs = parse_home_run(&batter_name, &event.description)?;
    let home_run_text = match num_runs {
        1 => { "solo".into() }
        num => { format!("{}-run", num) }
    };

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let message = format!("{} hits a {} home run!\nThe {} scored!", batter_name, home_run_text, team_name);
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, false)
        .chain(end_at_bat(game_id, top_of_inning))
        .chain(score_runs(game_id, top_of_inning, num_runs, "Home Run"))
        .chain([
            bs::Patch {
                path: bs::json_path!("player", player_id.clone(), "consecutiveHits"),
                change: bs::ChangeType::AddInt(1),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "lastUpdateFull", 1, "teamTags"),
                change: bs::ChangeType::Push(team_id.to_string().into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


async fn apply_storm_warning(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying StormWarning event for game {}", game_id)).await?;

    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let diff = common_patches(&event.metadata.siblings, game_id, "WINTER STORM WARNING".into(), play, false)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "gameStartPhase"),
                change: bs::ChangeType::Set(11.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


fn format_blaseball_date(date: DateTime<Utc>) -> String {
    // For some godforsaken reason, blaseball dates strip trailing millisecond zeros
    let main_date = date.format("%Y-%m-%dT%H:%M:%S");
    let millis = format!("{:0>3}", date.timestamp_millis() % 1000);
    let millis_trimmed = millis.trim_end_matches("0");
    let millis_final = if millis_trimmed.is_empty() { "0" } else { millis_trimmed };
    format!("{}.{}Z", main_date, millis_final)
}


async fn apply_snowflakes(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Snowflakes event for game {}", game_id)).await?;

    let play = event.metadata.play.ok_or(anyhow!("Missing metadata.play"))?;
    let (snow_event, mod_events) = event.metadata.siblings.split_first()
        .ok_or(anyhow!("Snowflakes event is missing metadata.siblings"))?;

    let (num_snowflakes, modified_type) = parse_snowfall(&snow_event.description)?;
    let frozen_player_ids: Vec<_> = mod_events.iter()
        .map(|event| {
            get_one_id(&event.player_tags, "playerTags")
        })
        .try_collect()?;

    let state_ref = &state;

    let frozen_messages: Vec<String> = stream::iter(frozen_player_ids.iter())
        .then(|uuid| async move {
            let name = state_ref.string_at(&bs::json_path!("player", *uuid.clone(), "name")).await?;
            Ok::<String, anyhow::Error>(format!("\n{} was Frozen!", name))
        })
        .try_collect().await?;

    let frozen_player_and_team_ids: Vec<_> = stream::iter(frozen_player_ids.into_iter().cloned())
        .then(|player_id| async move {
            let team_id = state_ref.uuid_at(&bs::json_path!("player", player_id.clone(), "leagueTeamId")).await?;
            Ok::<_, anyhow::Error>((player_id, team_id))
        })
        .try_collect().await?;

    let message = format!("{} Snowflakes {} the field!{}", num_snowflakes, modified_type, frozen_messages.join(""));
    let diff = common_patches(&event.metadata.siblings, game_id, message, play, true)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "gameStartPhase"),
                change: bs::ChangeType::Set(20.into()),
            },
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "state", "snowfallEvents"),
                change: bs::ChangeType::AddInt(1),
            },
        ])
        .chain(
            frozen_player_and_team_ids.into_iter()
                .enumerate()
                .map(|(i, (player_id, team_id))| {
                    [
                        bs::Patch {
                            path: bs::json_path!("game", game_id.clone(), "lastUpdateFull", i + 1, "teamTags"),
                            change: bs::ChangeType::Push(json!(team_id)),
                        },
                        bs::Patch {
                            path: bs::json_path!("player", player_id, "gameAttr"),
                            change: bs::ChangeType::Push(json!("FROZEN")),
                        }
                    ]
                })
                .flatten()
        );

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}


async fn apply_player_stat_reroll(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    log.debug(format!("Applying PlayerStatReroll event for player {}", player_id)).await?;

    let diff = [
        // TODO
    ].into_iter();

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
}