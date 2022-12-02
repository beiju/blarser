mod feed_event;
mod timed_event;
mod game_update;

// Events
mod start;
// mod earlseason_start;
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

pub use start::Start;
// pub use earlseason_start::EarlseasonStart;
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
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::state::Effects;

#[enum_dispatch]
pub trait Event: Serialize + for<'de> Deserialize<'de> {
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
    // EarlseasonStart(EarlseasonStart),
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
