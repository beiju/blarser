mod feed_event;
mod timed_event;
mod game_update;

// Events
mod start;
mod earlseason_start;
mod lets_go;
mod play_ball;
mod half_inning;
mod storm_warning;
mod batter_up;
mod parse_utils;
mod count_events;
mod fielding_outs;
mod hit;

pub use start::Start;
pub use earlseason_start::EarlseasonStart;
pub use lets_go::LetsGo;
pub use play_ball::PlayBall;
pub use half_inning::HalfInning;
pub use storm_warning::StormWarning;
pub use batter_up::BatterUp;
pub use count_events::{Strike, Ball, FoulBall, Strikeout};
pub use fielding_outs::{parse as parse_fielding_out, GroundOut, Flyout};
pub use hit::{Hit, HomeRun};

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
    StormWarning(StormWarning),
    BatterUp(BatterUp),
    Strike(Strike),
    Ball(Ball),
    FoulBall(FoulBall),
    Strikeout(Strikeout),
    GroundOut(GroundOut),
    Flyout(Flyout),
    Hit(Hit),
    HomeRun(HomeRun),
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
            crate::events::AnyEvent::StormWarning($bound_name) => { $arm }
            crate::events::AnyEvent::BatterUp($bound_name) => { $arm }
            crate::events::AnyEvent::Strike($bound_name) => { $arm }
            crate::events::AnyEvent::Ball($bound_name) => { $arm }
            crate::events::AnyEvent::FoulBall($bound_name) => { $arm }
            crate::events::AnyEvent::Strikeout($bound_name) => { $arm }
            crate::events::AnyEvent::GroundOut($bound_name) => { $arm }
            crate::events::AnyEvent::Flyout($bound_name) => { $arm }
            crate::events::AnyEvent::Hit($bound_name) => { $arm }
            crate::events::AnyEvent::HomeRun($bound_name) => { $arm }
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
            AnyEvent::StormWarning(_) => { "StormWarning" }
            AnyEvent::BatterUp(_) => { "BatterUp" }
            AnyEvent::Strike(_) => { "Strike" }
            AnyEvent::Ball(_) => { "Ball" }
            AnyEvent::FoulBall(_) => { "FoulBall" }
            AnyEvent::Strikeout(_) => { "Strikeout" }
            AnyEvent::GroundOut(_) => { "GroundOut" }
            AnyEvent::Flyout(_) => { "Flyout" }
            AnyEvent::Hit(_) => { "Hit" }
            AnyEvent::HomeRun(_) => { "HomeRun" }
        }
    }
}
