use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::Conflict;

use crate::entity::{AnyEntity, Entity};
use crate::events::{AnyExtrapolated, Effect, Event, Extrapolated, ord_by_time};
use crate::events::effects::BatterIdExtrapolated;
use crate::events::event_util::game_effect_with_batter_id;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;


#[derive(Debug, Serialize, Deserialize)]
pub struct Out {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Out {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            game_effect_with_batter_id(self.game_update.game_id, state)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &BatterIdExtrapolated = extrapolated.try_into().unwrap();
            game.team_at_bat_mut().batter = extrapolated.batter_id;

            self.game_update.forward(game);

            game.out(1);
        }
        entity
    }

    fn backward(&self, successor: &AnyEntity, extrapolated: &mut AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for Out {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Out for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(Out);