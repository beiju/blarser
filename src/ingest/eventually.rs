use std::sync::Arc;
use rocket::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use serde_json::json;
use uuid::Uuid;

use crate::api::{eventually, EventuallyEvent, EventType, LetsGoMetadata};
use crate::blaseball_state as bs;
use crate::blaseball_state::json_path;
use crate::ingest::{IngestError, IngestItem};
use crate::ingest::error::IngestResult;
use crate::ingest::log::IngestLogger;

pub fn sources(start: &'static str) -> Vec<Box<dyn Iterator<Item=Box<dyn IngestItem + Send>> + Send>> {
    vec![
        Box::new(eventually::events(start)
            .map(|event| Box::new(event) as Box<dyn IngestItem + Send>))
    ]
}

#[async_trait]
impl IngestItem for EventuallyEvent {
    fn date(&self) -> DateTime<Utc> {
        self.created
    }

    async fn apply(self: Box<Self>, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestResult {
        apply_feed_event(state, log, self).await
    }
}

pub async fn apply_feed_event(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: Box<EventuallyEvent>) -> IngestResult {
    log.debug(format!("Applying Feed event: {}", event.description)).await?;

    match event.r#type {
        EventType::BigDeal => apply_big_deal(state, log, event).await,
        EventType::LetsGo => apply_lets_go(state, log, event).await,
        EventType::PlayBall => apply_play_ball(state, log, event).await,
        EventType::HalfInning => apply_half_inning(state, log, event).await,
        EventType::BatterUp => apply_batter_up(state, log, event).await,
        EventType::Strike => apply_strike(state, log, event).await,
        EventType::Ball => apply_ball(state, log, event).await,
        EventType::FoulBall => apply_foul_ball(state, log, event).await,
        EventType::GroundOut => apply_ground_out(state, log, event).await,
        EventType::Hit => apply_hit(state, log, event).await,
        EventType::Strikeout => apply_strikeout(state, log, event).await,
        EventType::FlyOut => apply_fly_out(state, log, event).await,
        EventType::StolenBase => apply_stolen_base(state, log, event).await,
        EventType::Walk => apply_walk(state, log, event).await,
        EventType::RunsScored => apply_runs_scored(state, log, event).await,
        EventType::HomeRun => apply_home_run(state, log, event).await,
        _ => todo!()
    }
}

async fn apply_big_deal(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Ignoring BigDeal event".to_string()).await?;
    Ok(state)
}


async fn apply_lets_go(state: Arc<bs::BlaseballState>, log: &IngestLogger, event: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying LetsGo event".to_string()).await?;
    let metadata: LetsGoMetadata = serde_json::from_value(event.metadata)?;

    let diff = vec![
        bs::ValueChange::SetValue {
            path: json_path!("team", metadata.home, "rotationSlot"),
            value: json!(1)
        },
        bs::ValueChange::SetValue {
            path: json_path!("team", metadata.away, "rotationSlot"),
            value: json!(1)
        }
    ];

    state.successor(bs::Event::FeedEvent(event.id), diff)
}


async fn apply_play_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying PlayBall event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_half_inning(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying HalfInning event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_batter_up(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying BatterUp event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_strike(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying Strike event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying Ball event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_foul_ball(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying FoulBall event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_ground_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying GroundOut event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_hit(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying Hit event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_strikeout(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying Strikeout event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_fly_out(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying FlyOut event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_stolen_base(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying StolenBase event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_walk(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying Walk event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_runs_scored(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying RunsScored event".to_string()).await?;
    // TODO
    Ok(state)
}


async fn apply_home_run(state: Arc<bs::BlaseballState>, log: &IngestLogger, _: Box<EventuallyEvent>) -> IngestResult {
    log.debug("Applying HomeRun event".to_string()).await?;
    // TODO
    Ok(state)
}
