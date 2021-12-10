use std::future::Future;
use std::sync::Arc;
use rocket::async_trait;
use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;
use serde::Deserialize;

use crate::api::{eventually, EventuallyEvent, EventType, Weather};
use crate::blaseball_state as bs;
use crate::ingest::{IngestItem, BoxedIngestItem, IngestError, IngestResult};
use crate::ingest::error::IngestApplyResult;
use crate::ingest::log::IngestLogger;

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
        log.debug(format!("Applying Feed event: {}", self.description)).await?;

        match self.r#type {
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
            EventType::RunsScored => apply_runs_scored(state, log, self).await,
            EventType::HomeRun => apply_home_run(state, log, self).await,
            EventType::StormWarning => apply_storm_warning(state, log, self).await,
            EventType::PlayerStatReroll => apply_player_stat_reroll(state, log, self).await,
            EventType::Snowflakes => apply_snowflakes(state, log, self).await,
            EventType::AddedMod => apply_added_mod(state, log, self).await,
            _ => todo!()
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlayMetadata {
    pub play: i64,
}


async fn apply_big_deal(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Ignoring BigDeal event".to_string()).await?;
    Ok(vec![state])
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

    let metadata: LetsGoMetadata = serde_json::from_value(event.metadata.clone())?;
    let home_pitcher = get_active_pitcher(&state, metadata.home, event.day > 0).await?;
    let away_pitcher = get_active_pitcher(&state, metadata.away, event.day > 0).await?;

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
    ].into_iter()
        .chain(game_start_team_specific_diffs(game_id, away_pitcher, "away"))
        .chain(game_start_team_specific_diffs(game_id, home_pitcher, "home"));

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}

fn game_start_team_specific_diffs(game_id: &Uuid, active_pitcher: ActivePitcher, which: &'static str) -> impl Iterator<Item=bs::Patch> {
    [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}BatterName", which)),
            change: bs::ChangeType::Set("".to_string().into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Odds", which)),
            change: bs::ChangeType::Set(bs::PrimitiveValue::FloatRange(0., 1.)),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Pitcher", which)),
            change: bs::ChangeType::Set(active_pitcher.pitcher_id.to_string().into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}PitcherName", which)),
            change: bs::ChangeType::Set(active_pitcher.pitcher_name.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Score", which)),
            change: bs::ChangeType::Set(0.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Strikes", which)),
            change: bs::ChangeType::Set(3.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}TeamBatterCount", which)),
            change: bs::ChangeType::Set((-1).into()),
        },
    ].into_iter()
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

    let metadata: PlayMetadata = serde_json::from_value(event.metadata.clone())?;
    let diff = common_patches(event, game_id, "Play ball!".into(), metadata.play)
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
        .map(|s| vec![s])
}

fn prefixed(text: &'static str, top_of_inning: bool) -> String {
    let home_or_away = if top_of_inning { "away" } else { "home" };
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

fn common_patches(event: &EventuallyEvent, game_id: &Uuid, message: String, play: i64) -> impl Iterator<Item=bs::Patch> {
    [
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
            change: bs::ChangeType::Overwrite(json!([{
                "blurb": "",
                "category": event.category as i32,
                "created": format_blaseball_date(event.created),
                "day": event.day,
                "description": message,
                "gameTags": [],
                "id": event.id,
                "nuts": 0,
                "phase": 2,
                "playerTags": event.player_tags,
                "season": event.season,
                "teamTags": [],
                "tournament": event.tournament,
                "type": event.r#type as i32,
            }])),
        },
    ].into_iter()
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
    let metadata: PlayMetadata = serde_json::from_value(event.metadata.clone())?;
    let diff = common_patches(event, game_id, message, metadata.play)
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
            .map(|s| vec![s])
    } else {
        state.successor(caused_by, diff).await
            .map(|s| vec![s])
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

    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct BatterUpMetadata {
        pub play: i64,
    }

    let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    let batting_team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), prefixed("Team", top_of_inning))).await?;
    let batting_team_name = state.string_at(&bs::json_path!("team", batting_team_id, "nickname")).await?;
    let batter_count = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), prefixed("TeamBatterCount", top_of_inning))).await?;
    let batter_id = state.uuid_at(&bs::json_path!("team", batting_team_id, "lineup", batter_count as usize)).await?;
    let batter_name = state.string_at(&bs::json_path!("player", batter_id, "name")).await?;

    let metadata: BatterUpMetadata = serde_json::from_value(event.metadata.clone())?;
    let message = format!("{} batting for the {}.", batter_name, batting_team_name);
    let diff = common_patches(event, game_id, message, metadata.play)
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
        .map(|s| vec![s])
}


async fn apply_strike(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Strike event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying Ball event for game {}", game_id)).await?;

    // let top_of_inning = state.bool_at(&bs::json_path!("game", game_id.clone(), "topOfInning")).await?;
    // let max_balls = state.int_at(&bs::json_path!("game", game_id.clone(), prefixed("Balls", top_of_inning))).await?;

    let balls = 1 + state.int_at(&bs::json_path!("game", game_id.clone(), "atBatBalls")).await?;
    let strikes = state.int_at(&bs::json_path!("game", game_id.clone(), "atBatStrikes")).await?;

    log.debug(format!("Recording Ball for game {}, count {}-{}", game_id, balls, strikes)).await?;

    let metadata: PlayMetadata = serde_json::from_value(event.metadata.clone())?;
    let message = format!("Ball. {}-{}", balls, strikes);
    let diff = common_patches(event, game_id, message, metadata.play)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "atBatBalls"),
                change: bs::ChangeType::Set(balls.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}


async fn apply_foul_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying FoulBall event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_ground_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying GroundOut event".to_string()).await?;

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let player_id = todo!(); // It's not in the object. Fuck
    let diff = apply_out("ground out", log, player_id).await?;

    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}


async fn apply_hit(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Hit event".to_string()).await?;

    let player_id = get_one_id(&event.player_tags, "playerTags")?;

    log.info(format!("Observed hit by {}. Changing consecutiveHits", player_id)).await?;
    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let diff = [
        bs::Patch {
            path: bs::json_path!("player", player_id.clone(), "consecutiveHits"),
            change: bs::ChangeType::Increment,
        },
    ];

    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}


async fn apply_strikeout(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Strikeout event".to_string()).await?;

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let player_id = get_one_id(&event.player_tags, "playerTags")?;
    let diff = apply_out("strikeout", log, player_id).await?;

    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}

fn apply_out<'a>(out_type: &'static str, log: &'a IngestLogger, player_id: &'a Uuid) -> impl Future<Output=IngestResult<Vec<bs::Patch>>> + 'a {
    async move {
        log.info(format!("Observed {} by {}. Zeroing consecutiveHits", out_type, player_id)).await?;
        let diff = vec![
            bs::Patch {
                path: bs::json_path!("player", player_id.clone(), "consecutiveHits"),
                change: bs::ChangeType::Set(0.into()),
            },
        ];

        Ok(diff)
    }
}

fn get_one_id<'a>(tags: &'a Vec<Uuid>, field_name: &'static str) -> IngestResult<&'a Uuid> {
    if tags.len() != 1 {
        return Err(IngestError::BadEvent(
            format!("Expected exactly one element in {} but found {}", field_name, tags.len())
        ));
    }

    tags.get(0)
        .ok_or_else(|| IngestError::BadEvent(
            format!("Expected exactly one element in {} but found none", field_name)))
}


async fn apply_fly_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying FlyOut event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_stolen_base(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying StolenBase event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_walk(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Walk event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_runs_scored(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying RunsScored event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_home_run(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying HomeRun event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_storm_warning(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    log.debug(format!("Applying StormWarning event for game {}", game_id)).await?;

    let game_id = get_one_id(&event.game_tags, "gameTags")?;
    #[derive(Deserialize, Debug)]
    #[serde(rename_all = "camelCase")]
    pub struct StormWarningMetadata {
        pub play: i64,
    }

    let metadata: StormWarningMetadata = serde_json::from_value(event.metadata.clone())?;
    let diff = common_patches(event, game_id, "WINTER STORM WARNING".into(), metadata.play)
        .chain([
            bs::Patch {
                path: bs::json_path!("game", game_id.clone(), "gameStartPhase"),
                change: bs::ChangeType::Set(11.into()),
            },
        ]);

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}


fn format_blaseball_date(date: DateTime<Utc>) -> String {
    // For some godforsaken reason, blaseball dates strip trailing millisecond zeros
    let main_date = date.format("%Y-%m-%dT%H:%M:%S");
    let millis = format!("{:0>3}", date.timestamp_millis() % 1000);
    let millis_trimmed = millis.trim_end_matches("0");
    let millis_final = if millis_trimmed.is_empty() { "0" } else { millis_trimmed };
    format!("{}.{}Z", main_date, millis_final)
}


async fn apply_player_stat_reroll(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying PlayerStatReroll event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_snowflakes(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Snowflakes event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_added_mod(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying AddedMod event".to_string()).await?;
    // TODO
    Ok(vec![state])
}
