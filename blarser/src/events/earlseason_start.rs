use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::{Conflict, DatetimeWithResettingMs, MaybeKnown, PartialInformationCompare};

use crate::entity::{AnyEntity};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::NullExtrapolated;
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

    fn forward(&self, mut entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
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

    fn backward(&self, successor: &AnyEntity, extrapolated: &mut AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict> {
        // I have decided not to record anything in extrapolated, even though there's changed data
        // to record, because I don't expect it to be useful. I don't even know if you'll even be
        // able to query for timed events' extrapolated data.
        // Still going to check the type of extrapolated though for completeness
        let _: &mut NullExtrapolated = extrapolated.try_into()
            .expect("Got the wrong Extrapolated type in EarlseasonStart.backward");

        let mut conflicts = Vec::new();
        match (successor.as_sim(), entity.as_sim_mut()) {
            (Some(succcessor), Some(parent)) => {
                if let Some(date) = succcessor.gods_day_date.known_date() {
                    conflicts.extend(parent.gods_day_date.observe(&date))
                }
                if let Some(date) = succcessor.next_phase_time.known_date() {
                    conflicts.extend(parent.next_phase_time.observe(&date))
                }
            }
            (None, None) => {}
            _ => {
                panic!("Mismatched entity types passed to EarlseasonStart.backward")
            }
        }
        
        match (successor.as_game(), entity.as_game_mut()) {
            (Some(succcessor), Some(parent)) => {
                if let Some(MaybeKnown::Known(successor_odds)) = &succcessor.home.odds {
                    if let Some(parent_odds) = parent.home.odds.as_mut() {
                        conflicts.extend(parent_odds.observe(&successor_odds))
                    }
                }
                if let Some(MaybeKnown::Known(successor_odds)) = &succcessor.away.odds {
                    if let Some(parent_odds) = parent.away.odds.as_mut() {
                        conflicts.extend(parent_odds.observe(&successor_odds))
                    }
                }
                if let Some(MaybeKnown::Known(successor_pitcher)) = &succcessor.home.pitcher {
                    if let Some(parent_pitcher) = parent.home.pitcher.as_mut() {
                        conflicts.extend(parent_pitcher.observe(&successor_pitcher))
                    }
                }
                if let Some(MaybeKnown::Known(successor_pitcher)) = &succcessor.away.pitcher {
                    if let Some(parent_pitcher) = parent.away.pitcher.as_mut() {
                        conflicts.extend(parent_pitcher.observe(&successor_pitcher))
                    }
                }
                if let Some(MaybeKnown::Known(successor_pitcher_name)) = &succcessor.home.pitcher_name {
                    if let Some(parent_pitcher_name) = parent.home.pitcher_name.as_mut() {
                        conflicts.extend(parent_pitcher_name.observe(&successor_pitcher_name))
                    }
                }
                if let Some(MaybeKnown::Known(successor_pitcher_name)) = &succcessor.away.pitcher_name {
                    if let Some(parent_pitcher_name) = parent.away.pitcher_name.as_mut() {
                        conflicts.extend(parent_pitcher_name.observe(&successor_pitcher_name))
                    }
                }
            }
            (None, None) => {}
            _ => {
                panic!("Mismatched entity types passed to EarlseasonStart.backward")
            }
        }

        Vec::new()
    }
}

impl Display for EarlseasonStart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "EarlseasonStart at {}", self.time)
    }
}

ord_by_time!(EarlseasonStart);