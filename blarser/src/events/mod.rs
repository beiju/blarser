mod feed_event_old;
mod timed_event;
mod game_update;

// Events
mod start;
mod earlseason_start;
mod feed_event;
// mod lets_go;
// mod play_ball;
// mod half_inning;
// mod storm_warning;
// mod batter_up;
// mod parse_utils;
// mod count_events;
// mod fielding_outs;
// mod hit;
// mod player_reroll;

use std::fmt::{Display, Formatter};
pub use start::Start;
pub use earlseason_start::EarlseasonStart;
pub use feed_event::FeedEvent;
// pub use lets_go::LetsGo;
// pub use play_ball::PlayBall;
// pub use half_inning::HalfInning;
// pub use storm_warning::StormWarning;
// pub use batter_up::BatterUp;
// pub use count_events::{Strike, Ball, FoulBall, Strikeout};
// pub use fielding_outs::{parse as parse_fielding_out, GroundOut, Flyout};
// pub use hit::{Hit, HomeRun};
// pub use player_reroll::{parse as parse_player_reroll, Snow};

use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use fed::FedEvent;
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::state::Effects;

#[enum_dispatch]
pub trait Event: Serialize + for<'de> Deserialize<'de> + Ord + Display {
    fn time(&self) -> DateTime<Utc>;

    fn generate_successors(&self) -> Vec<(AnyEvent, Effects)> {
        Vec::new()
    }

    fn forward(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity;
    fn reverse(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity;
}

#[enum_dispatch(Event)]
#[derive(Serialize, Deserialize)]
pub enum AnyEvent {
    Start(Start),
    EarlseasonStart(EarlseasonStart),
    FeedEvent(FeedEvent),
    // LetsGo(LetsGo),
    // PlayBall(PlayBall),
    // HalfInning(HalfInning),
    // StormWarning(StormWarning),
    // BatterUp(BatterUp),
    // Strike(Strike),
    // Ball(Ball),
    // FoulBall(FoulBall),
    // Strikeout(Strikeout),
    // GroundOut(GroundOut),
    // Flyout(Flyout),
    // Hit(Hit),
    // HomeRun(HomeRun),
    // Snow(Snow),
}

impl Display for AnyEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyEvent::Start(e) => { e.fmt(f) }
            AnyEvent::EarlseasonStart(e) => { e.fmt(f) }
            AnyEvent::FeedEvent(e) => { e.fmt(f) }
        }
    }
}

macro_rules! ord_by_time {
    ($tn:ty) => {
        impl Eq for $tn {}

        impl PartialEq<Self> for $tn {
            fn eq(&self, other: &Self) -> bool {
                self.time().eq(&other.time())
            }
        }

        impl PartialOrd<Self> for $tn {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                self.time().partial_cmp(&other.time())
            }
        }

        impl Ord for $tn {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.time().cmp(&other.time())
            }
        }
    }
}

ord_by_time!(AnyEvent);

pub(crate) use ord_by_time;