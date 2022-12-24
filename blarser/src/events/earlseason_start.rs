use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::{DatetimeWithResettingMs, MaybeKnown};

use crate::entity::{AnyEntity, Sim};
use crate::events::{AnyEvent, AnyExtrapolated, Effect, Event, GameUpcoming, ord_by_time};
use crate::events::effects::EarlseasonStartSubsecondsExtrapolated;
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

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::null_id_with(EntityType::Sim, EarlseasonStartSubsecondsExtrapolated::default()),
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();

        if let Some(sim) = entity.as_sim_mut() {
            let extrapolated: &EarlseasonStartSubsecondsExtrapolated = extrapolated.try_into()
                .expect("Mismatched extrapolated type");
            if sim.phase == 1 {
                sim.phase = 2;
                sim.next_phase_time = DatetimeWithResettingMs::from_without_ms(sim.earlseason_date);
                sim.next_phase_time.maybe_set_ns(extrapolated.next_phase_ns);
                sim.gods_day_date.maybe_set_ns(extrapolated.gods_day_ns);
            } else {
                panic!("Tried to apply EarlseasonStart event while not in Preseason phase")
            }
        }

        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Sim(old_sim) => {
                let new_sim: &mut Sim = new_parent.try_into()
                    .expect("Mismatched event types");
                let extrapolated: &mut EarlseasonStartSubsecondsExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");
                extrapolated.next_phase_ns = new_sim.next_phase_time.ns();
                extrapolated.gods_day_ns = new_sim.gods_day_date.ns();

                if new_sim.phase == 2 {
                    new_sim.phase = 1;
                    new_sim.gods_day_date = old_sim.gods_day_date;
                    new_sim.next_phase_time = old_sim.next_phase_time;
                } else {
                    panic!("Tried to reverse-apply EarlseasonStart event while not in Earlseason phase")
                }
            }
            _ => {
                panic!("Can't reverse-apply EarlseasonStart to this entity type");
            }
        }
    }
}

impl Display for EarlseasonStart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EarlseasonStart at {}", self.time)
    }
}

ord_by_time!(EarlseasonStart);