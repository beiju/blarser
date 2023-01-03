mod feed_event_old;
mod timed_event;
mod effects;
mod event_util;

// Events
mod start;
mod earlseason_start;
mod fed_event;
// mod lets_go;
// mod play_ball;
// mod half_inning;
// mod toggle_performing;
// mod storm_warning;
// mod batter_up;
// mod count_events;
// mod out;
// mod hit;
// mod stolen_base;
// mod walk;
mod game_upcoming;
// mod inning_end;
// mod player_reroll;

pub use effects::{Extrapolated, AnyExtrapolated, Effect, AnyEffect, EffectVariant, AnyEffectVariant};
pub(crate) use effects::with_effect_variant;
pub use start::Start;
pub use earlseason_start::{EarlseasonStart, EarlseasonStartEffect, EarlseasonStartEffectVariant};
pub use fed_event::*;
// pub use lets_go::LetsGo;
// pub use play_ball::PlayBall;
// pub use toggle_performing::TogglePerforming;
// pub use half_inning::HalfInning;
// pub use storm_warning::StormWarning;
// pub use batter_up::BatterUp;
// pub use count_events::{Strike, Ball, FoulBall};
// pub use out::{CaughtOut, FieldersChoice, Strikeout};
// pub use hit::{Hit, HomeRun};
// pub use stolen_base::{StolenBase, CaughtStealing};
// pub use walk::Walk;
pub use game_upcoming::{GameUpcoming, GameUpcomingEffect, GameUpcomingEffectVariant};
// pub use inning_end::InningEnd;

use crate::polymorphic_enum::polymorphic_enum;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use derive_more::{From, TryInto};

use crate::ingest::StateGraph;

pub trait Event: Serialize + for<'de> Deserialize<'de> {
    fn time(&self) -> DateTime<Utc>;

    // "Predecessors" are events that occur immediately before this event occurs, but their timing
    // isn't known until this event is received. This can be used to fill in for invisible events.
    // This function will be called, and the resulting event applied, until it returns None.
    #[allow(unused_variables)]
    fn generate_predecessor(&self, state: &StateGraph) -> Option<AnyEvent> {
        None
    }

    // "Successors" are events that are generated when this event occurs. Typically they are timed
    // events scheduled for some time in the future. This can be used to fill in for known-missing
    // Feed events.
    #[allow(unused_variables)]
    fn generate_successors(&self, state: &StateGraph) -> Vec<AnyEvent> {
        Vec::new()
    }

    fn into_effects(self, state: &StateGraph) -> Vec<AnyEffect>;
}
polymorphic_enum!{
    #[derive(Debug, Serialize, Deserialize, TryInto, From)]
    #[try_into(owned, ref, ref_mut)]
    pub AnyEvent: with_any_event {
        // These need to use absolute paths for the exported macro to work
        Start(crate::events::Start),
        EarlseasonStart(crate::events::EarlseasonStart),
        GameUpcoming(crate::events::GameUpcoming),
        Fed(crate::events::FedEvent),
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

    pub fn generate_predecessor(&self, state: &StateGraph) -> Option<AnyEvent> {
        with_any_event!(self, |e| { e.generate_predecessor(state) })
    }

    pub fn generate_successors(&self, state: &StateGraph) -> Vec<AnyEvent> {
        with_any_event!(self, |e| { e.generate_successors(state) })
    }

    pub fn into_effects(self, state: &StateGraph) -> Vec<AnyEffect> {
        with_any_event!(self, |e| { e.into_effects(state) })
    }
}
