use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::{Conflict, DatetimeWithResettingMs, MaybeKnown, PartialInformationCompare};

use crate::entity::{AnyEntity};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::{OddsExtrapolated, SubsecondsExtrapolated};
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct EarlseasonStart {
    time: DateTime<Utc>,
}

impl EarlseasonStart {
    pub fn new(time: DateTime<Utc>) -> Self {
        EarlseasonStart { time }
    }
}

impl Event for EarlseasonStart {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::null_id(EntityType::Sim),
            Effect::all_ids(EntityType::Game),
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();

        if let Some(sim) = entity.as_sim_mut() {
            if sim.phase == 1 {
                sim.phase = 2;
                sim.gods_day_date.forget_ms();
                sim.next_phase_time = DatetimeWithResettingMs::from_without_ms(sim.earlseason_date);
            } else {
                panic!("Tried to apply EarlseasonStart event while not in Preseason phase")
            }
        } else if let Some(game) = entity.as_game_mut() {
            for self_by_team in [&mut game.home, &mut game.away] {
                self_by_team.batter_name = Some(String::new());
                self_by_team.odds = Some(MaybeKnown::Unknown);
                self_by_team.pitcher = Some(MaybeKnown::Unknown);
                self_by_team.pitcher_name = Some(MaybeKnown::Unknown);
                self_by_team.score = Some(0.0);
                self_by_team.strikes = Some(3);
            }
            game.last_update = Some(String::new());
            game.last_update_full = Some(Vec::new());
        }

        entity
    }

    fn backward(&self, extrapolated: &AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict> {
        let mut conflicts = Vec::new();

        if let Some(sim) = entity.as_sim_mut() {
            let ns_extrapolated: &SubsecondsExtrapolated = extrapolated.try_into()
                .expect("Got the wrong Extrapolated type in EarlseasonStart.backward");

            if let MaybeKnown::Known(ns) = ns_extrapolated.ns {
                sim.gods_day_date.set_ns(ns);
                sim.next_phase_time.set_ns(ns);
            }
        }

        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &OddsExtrapolated = extrapolated.try_into()
                .expect("Got the wrong Extrapolated type in EarlseasonStart.backward");

            if let MaybeKnown::Known(odd) = extrapolated.away_odds {
                conflicts.extend(game.away.odds.observe(&Some(odd)));
            }
            if let MaybeKnown::Known(odd) = extrapolated.home_odds {
                conflicts.extend(game.home.odds.observe(&Some(odd)));
            }
        }

        conflicts
    }
}

impl Display for EarlseasonStart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EarlseasonStart at {}", self.time)
    }
}

ord_by_time!(EarlseasonStart);