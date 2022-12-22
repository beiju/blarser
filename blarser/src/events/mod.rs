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
mod stolen_base;
mod walk;
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
pub use stolen_base::{StolenBase, CaughtStealing};
pub use walk::Walk;

use std::fmt::{Display, Formatter};
use as_any::{AsAny, Downcast};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use derive_more::{From, TryInto};

use crate::entity::{AnyEntity, Entity};
use crate::ingest::StateGraph;
use partial_information::Conflict;

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
    fn fill_extrapolated(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyExtrapolated {
        if let AnyExtrapolated::Null(_) = extrapolated {
            AnyExtrapolated::Null(NullExtrapolated {})
        } else {
            panic!("Cannot use default fill_extrapolated on {}", extrapolated.type_name())
        }
    }
    fn backward(&self, extrapolated: &AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict>;

}

polymorphic_enum!{
    #[derive(Debug, Serialize, Deserialize, TryInto, From)]
    #[try_into(owned, ref, ref_mut)]
    pub AnyEvent: with_any_event {
        // These need to use absolute paths for the exported macro to work
        Start(crate::events::Start),
        EarlseasonStart(crate::events::EarlseasonStart),
        LetsGo(crate::events::LetsGo),
        PlayBall(crate::events::PlayBall),
        TogglePerforming(crate::events::TogglePerforming),
        HalfInning(crate::events::HalfInning),
        StormWarning(crate::events::StormWarning),
        BatterUp(crate::events::BatterUp),
        Strike(crate::events::Strike),
        Ball(crate::events::Ball),
        FoulBall(crate::events::FoulBall),
        Out(crate::events::Out),
        Hit(crate::events::Hit),
        HomeRun(crate::events::HomeRun),
        StolenBase(crate::events::StolenBase),
        Walk(crate::events::Walk),
        CaughtStealing(crate::events::CaughtStealing),
    }
}

pub(crate) use with_any_event;

impl Display for AnyEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        with_any_event!(self, |e| { e.fmt(f) })
    }
}

impl AnyEvent {
    pub fn time(&self) -> DateTime<Utc> {
        with_any_event!(self, |e| { e.time() })
    }
    pub fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        with_any_event!(self, |e| { e.effects(state) })
    }
    pub fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        with_any_event!(self, |e| { e.forward(entity, extrapolated) })
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
use crate::events::effects::NullExtrapolated;
use crate::polymorphic_enum::polymorphic_enum;
