mod feed_event;
mod timed_event;
mod game_update;

// Events
mod start;
mod earlseason_start;
mod lets_go;
mod play_ball;
mod half_inning;

pub use start::Start;
pub use earlseason_start::EarlseasonStart;
pub use lets_go::LetsGo;
pub use play_ball::PlayBall;
pub use half_inning::HalfInning;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;

pub trait Event: Serialize + for<'de> Deserialize<'de> {
    fn time(&self) -> DateTime<Utc>;

    fn forward(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity;
    fn reverse(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity;
}

#[derive(Serialize, Deserialize)]
pub enum AnyEvent {
    Start(Start),
    EarlseasonStart(EarlseasonStart),
    LetsGo(LetsGo),
    PlayBall(PlayBall),
    HalfInning(HalfInning),
}

#[macro_export]
macro_rules! with_any_event {
    ($any_event:expr, $bound_name:ident => $arm:expr) => {
        match $any_event {
            crate::events::AnyEvent::Start($bound_name) => { $arm }
            crate::events::AnyEvent::EarlseasonStart($bound_name) => { $arm }
            crate::events::AnyEvent::LetsGo($bound_name) => { $arm }
            crate::events::AnyEvent::PlayBall($bound_name) => { $arm }
            crate::events::AnyEvent::HalfInning($bound_name) => { $arm }
        }
    };
}

pub use with_any_event;

impl AnyEvent {
    pub fn time(&self) -> DateTime<Utc> {
        with_any_event!(self, event => event.time())
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            AnyEvent::Start(_) => { "Start" }
            AnyEvent::EarlseasonStart(_) => { "EarlseasonStart" }
            AnyEvent::LetsGo(_) => { "LetsGo" }
            AnyEvent::PlayBall(_) => { "PlayBall" }
            AnyEvent::HalfInning(_) => { "HalfInning" }
        }
    }
}