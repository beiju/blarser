use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{DatetimeWithResettingMs, MaybeKnown};

use crate::entity::{AnyEntity, Sim};
use crate::events::{AnyEvent, AnyExtrapolated, Effect, Event, GameUpcoming};
use crate::events::effects::{AnyEffect, EarlseasonStartSubsecondsExtrapolated, EffectVariant};
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct EarlseasonStart {
    time: DateTime<Utc>,
    season: i32,
}

impl EarlseasonStart {
    pub fn new(time: DateTime<Utc>, season: i32) -> Self {
        EarlseasonStart { time, season }
    }
}

impl Event for EarlseasonStart {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn generate_successors(&self, state: &StateGraph) -> Vec<AnyEvent> {
        let day0_successors = state.games_for_day(self.season, 0)
            .map(|game_id| {
                GameUpcoming::new(self.time, game_id).into()
            });
        let day1_successors = state.games_for_day(self.season, 1)
            .map(|game_id| {
                GameUpcoming::new(self.time, game_id).into()
            });
        day0_successors.chain(day1_successors).collect()
    }

    fn into_effects(self, _: &StateGraph) -> Vec<AnyEffect> {
        vec![
            EarlseasonStartEffect.into(),
        ]
    }
}

impl Display for EarlseasonStart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EarlseasonStart at {}", self.time)
    }
}

#[derive(Clone, Debug)]
pub struct EarlseasonStartEffect;

impl Effect for EarlseasonStartEffect {
    type Variant = EarlseasonStartEffectVariant;

    fn entity_type(&self) -> EntityType { EntityType::Sim }

    fn entity_id(&self) -> Option<Uuid> { Some(Uuid::nil()) }

    fn variant(&self) -> Self::Variant {
        EarlseasonStartEffectVariant::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct EarlseasonStartEffectVariant {
    next_phase_ns: MaybeKnown<u32>,
    gods_day_ns: MaybeKnown<u32>,
}

impl EffectVariant for EarlseasonStartEffectVariant {
    type EntityType = Sim;

    fn forward(&self, sim: &mut Sim) {
        if sim.phase == 1 {
            sim.phase = 2;
            sim.next_phase_time = DatetimeWithResettingMs::from_without_ms(sim.earlseason_date);
            sim.next_phase_time.maybe_set_ns(self.next_phase_ns);
            sim.gods_day_date.maybe_set_ns(self.gods_day_ns);
        } else {
            panic!("Tried to apply EarlseasonStart event while not in Preseason phase")
        }
    }

    fn reverse(&mut self, old_sim: &Sim, new_sim: &mut Sim) {
        self.next_phase_ns = new_sim.next_phase_time.ns();
        self.gods_day_ns = new_sim.gods_day_date.ns();

        if new_sim.phase == 2 {
            new_sim.phase = 1;
            new_sim.gods_day_date = old_sim.gods_day_date;
            new_sim.next_phase_time = old_sim.next_phase_time;
        } else {
            panic!("Tried to reverse-apply EarlseasonStart event while not in Earlseason phase")
        }
    }
}
