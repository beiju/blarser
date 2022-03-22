use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::MaybeKnown;

use crate::entity::AnyEntity;
use crate::events::Event;

#[derive(Serialize, Deserialize)]
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

    fn forward(&self, mut entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match &mut entity {
            AnyEntity::Sim(sim) => {
                if sim.phase == 1 {
                    sim.phase = 2;
                    sim.next_phase_time = sim.earlseason_date;
                } else {
                    panic!("Tried to apply EarlseasonStart event while not in Preseason phase")
                }
            }
            AnyEntity::Game(game) => {
                for self_by_team in [&mut game.home, &mut game.away] {
                    self_by_team.batter_name = Some(String::new());
                    self_by_team.odds = Some(MaybeKnown::Unknown);
                    self_by_team.pitcher = Some(MaybeKnown::Unknown);
                    self_by_team.pitcher_name = Some(MaybeKnown::Unknown);
                    self_by_team.score = Some(0.0);
                    self_by_team.strikes = Some(3);
                }
                game.last_update = String::new();
                game.last_update_full = Some(Vec::new());
            }
            other => panic!("EarlseasonStart event does not apply to {}", other.name())
        }

        entity
    }

    fn reverse(&self, _: AnyEntity, _: serde_json::Value) -> AnyEntity {
        todo!()
    }
}