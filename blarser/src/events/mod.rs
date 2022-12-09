mod feed_event_old;
mod timed_event;
mod game_update;
mod effects;
mod event_util;

// Events
mod start;
mod earlseason_start;
mod lets_go;
mod play_ball;
mod half_inning;
mod toggle_performing;
mod storm_warning;
mod batter_up;
mod count_events;
mod out;
mod hit;
// mod player_reroll;

pub(crate) use game_update::GameUpdate;
pub use effects::{Extrapolated, AnyExtrapolated, Effect};
pub use start::Start;
pub use earlseason_start::EarlseasonStart;
pub use lets_go::LetsGo;
pub use play_ball::PlayBall;
pub use toggle_performing::TogglePerforming;
pub use half_inning::HalfInning;
pub use storm_warning::StormWarning;
pub use batter_up::BatterUp;
pub use count_events::{Strike, Ball, FoulBall};
pub use out::Out;
pub use hit::{Hit, HomeRun};

use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use crate::entity::AnyEntity;
use crate::ingest::StateGraph;

#[enum_dispatch]
pub trait Event: Serialize + for<'de> Deserialize<'de> + Ord + Display {
    fn time(&self) -> DateTime<Utc>;

    // "Successors" are events that are generated when this event occurs. Typically they are timed
    // events scheduled for some time in the future. This can be used to fill in for known-missing
    // Feed events.
    fn generate_successors(&self) -> Vec<AnyEvent> {
        Vec::new()
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect>;

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity;
    fn reverse(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity;
}

#[enum_dispatch(Event)]
#[derive(Debug, Serialize, Deserialize)]
pub enum AnyEvent {
    Start(Start),
    EarlseasonStart(EarlseasonStart),
    LetsGo(LetsGo),
    PlayBall(PlayBall),
    TogglePerforming(TogglePerforming),
    HalfInning(HalfInning),
    StormWarning(StormWarning),
    BatterUp(BatterUp),
    Strike(Strike),
    Ball(Ball),
    FoulBall(FoulBall),
    Out(Out),
    Hit(Hit),
    HomeRun(HomeRun),
}

impl Display for AnyEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyEvent::Start(e) => { e.fmt(f) }
            AnyEvent::EarlseasonStart(e) => { e.fmt(f) }
            AnyEvent::LetsGo(e) => { e.fmt(f) }
            AnyEvent::PlayBall(e) => { e.fmt(f) }
            AnyEvent::TogglePerforming(e) => { e.fmt(f) }
            AnyEvent::HalfInning(e) => { e.fmt(f) }
            AnyEvent::StormWarning(e) => { e.fmt(f) }
            AnyEvent::BatterUp(e) => { e.fmt(f) }
            AnyEvent::Strike(e) => { e.fmt(f) }
            AnyEvent::Ball(e) => { e.fmt(f) }
            AnyEvent::FoulBall(e) => { e.fmt(f) }
            AnyEvent::Out(e) => { e.fmt(f) }
            AnyEvent::Hit(e) => { e.fmt(f) }
            AnyEvent::HomeRun(e) => { e.fmt(f) }
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
