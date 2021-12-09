use std::future::Future;
use std::sync::Arc;
use rocket::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::api::{eventually, EventuallyEvent, EventType, LetsGoMetadata};
use crate::blaseball_state as bs;
use crate::blaseball_state::{Event, json_path, Patch};
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

async fn apply_big_deal(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Ignoring BigDeal event".to_string()).await?;
    Ok(vec![state])
}


async fn apply_lets_go(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying LetsGo event".to_string()).await?;
    let metadata: LetsGoMetadata = serde_json::from_value(event.metadata.clone())?;

    let mut games: Vec<_> = state.data.get("game").unwrap().keys()
        .into_iter()
        .map(|uuid| format!("{}", uuid))
        .collect();

    games.sort_unstable();

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let diff = vec![
        bs::Patch {
            path: json_path!("team", metadata.home, "rotationSlot"),
            change: bs::ChangeType::Increment,
        },
        bs::Patch {
            path: json_path!("team", metadata.away, "rotationSlot"),
            change: bs::ChangeType::Increment,
        },
    ];

    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}


async fn apply_play_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying PlayBall event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_half_inning(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying HalfInning event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_batter_up(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying BatterUp event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_strike(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Strike event".to_string()).await?;
    // TODO
    Ok(vec![state])
}


async fn apply_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Ball event".to_string()).await?;
    // TODO
    Ok(vec![state])
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

    let player_id = get_one_player_id(event)?;

    log.info(format!("Observed hit by {}. Changing consecutiveHits", player_id)).await?;
    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let diff = vec![
        bs::Patch {
            path: json_path!("player", player_id.clone(), "consecutiveHits"),
            change: bs::ChangeType::Increment,
        },
    ];

    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}


async fn apply_strikeout(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying Strikeout event".to_string()).await?;

    let caused_by = Arc::new(bs::Event::FeedEvent(event.id));
    let player_id = get_one_player_id(event)?;
    let diff = apply_out("strikeout", log, player_id).await?;

    state.successor(caused_by, diff).await
        .map(|s| vec![s])
}

fn apply_out<'a>(out_type: &'static str, log: &'a IngestLogger, player_id: &'a Uuid) -> impl Future<Output=IngestResult<Vec<Patch>>> + 'a {
    async move {
        log.info(format!("Observed {} by {}. Zeroing consecutiveHits", out_type, player_id)).await?;
        let diff = vec![
            bs::Patch {
                path: json_path!("player", player_id.clone(), "consecutiveHits"),
                change: bs::ChangeType::Replace(0.into()),
            },
        ];

        Ok(diff)
    }
}

fn get_one_player_id(event: &EventuallyEvent) -> IngestResult<&Uuid> {
    if event.player_tags.len() != 1 {
        return Err(IngestError::BadEvent(
            format!("Expected exactly one element in playerTags but found {}", event.player_tags.len())
        ));
    }

    event.player_tags.get(0)
        .ok_or_else(|| IngestError::BadEvent("Expected exactly one element in playerTags but found none".to_string()))
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


async fn apply_storm_warning(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: &EventuallyEvent) -> IngestApplyResult {
    log.debug("Applying StormWarning event".to_string()).await?;
    // TODO
    Ok(vec![state])
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
