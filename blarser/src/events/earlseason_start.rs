use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::{DatetimeWithResettingMs, MaybeKnown, PartialInformationCompare};

use crate::entity::{AnyEntity, Game, Sim};
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
            Effect::null_id_with(EntityType::Sim, SubsecondsExtrapolated::default()),
            Effect::all_ids_with(EntityType::Game, OddsExtrapolated::default()),
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();

        if let Some(sim) = entity.as_sim_mut() {
            let extrapolated: &SubsecondsExtrapolated = extrapolated.try_into()
                .expect("Mismatched extrapolated type");
            if sim.phase == 1 {
                sim.phase = 2;
                sim.next_phase_time = DatetimeWithResettingMs::from_without_ms(sim.earlseason_date);
                if let MaybeKnown::Known(ns) = extrapolated.ns {
                    sim.gods_day_date.set_ns(ns);
                    sim.next_phase_time.set_ns(ns);
                } else {
                    sim.gods_day_date.forget_ms();
                }
            } else {
                panic!("Tried to apply EarlseasonStart event while not in Preseason phase")
            }
        } else if let Some(game) = entity.as_game_mut() {
            let extrapolated: &OddsExtrapolated = extrapolated.try_into()
                .expect("Mismatched extrapolated type");
            for (self_by_team, odds_extrapolated) in [
                (&mut game.home, extrapolated.home_odds),
                (&mut game.away, extrapolated.away_odds)
            ] {
                self_by_team.batter_name = Some(String::new());
                self_by_team.odds = Some(odds_extrapolated);
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

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Sim(old_sim) => {
                let new_sim: &mut Sim = new_parent.try_into()
                    .expect("Mismatched event types");
                let extrapolated: &mut SubsecondsExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");
                extrapolated.ns = new_sim.next_phase_time.ns();

                if new_sim.phase == 2 {
                    new_sim.phase = 1;
                    new_sim.gods_day_date = old_sim.gods_day_date;
                    new_sim.next_phase_time = old_sim.next_phase_time;
                } else {
                    panic!("Tried to reverse-apply EarlseasonStart event while not in Earlseason phase")
                }
            }
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched event types");
                let extrapolated: &mut OddsExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");
                extrapolated.away_odds = new_game.away.odds
                    .expect("Odds should exist when reversing an EarlseasonStart event");
                extrapolated.home_odds = new_game.home.odds
                    .expect("Odds should exist when reversing an EarlseasonStart event");

                for (old_by_team, new_by_team) in [
                    (&old_game.home, &mut new_game.home),
                    (&old_game.away, &mut new_game.away),
                ] {
                    new_by_team.batter_name = old_by_team.batter_name.clone();
                    new_by_team.odds = old_by_team.odds;
                    new_by_team.pitcher = old_by_team.pitcher;
                    new_by_team.pitcher_name = old_by_team.pitcher_name.clone();
                    new_by_team.score = old_by_team.score;
                    new_by_team.strikes = old_by_team.strikes;
                }
                new_game.last_update = old_game.last_update.clone();
                new_game.last_update_full = old_game.last_update_full.clone();
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