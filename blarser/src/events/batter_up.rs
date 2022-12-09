use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::BatterIdExtrapolated;
use crate::events::event_util::game_effect_with_next_batter_id;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;

#[derive(Debug, Serialize, Deserialize)]
pub struct BatterUp {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) batter_name: String,
}

impl Event for BatterUp {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            game_effect_with_next_batter_id(self.game_update.game_id, state)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        // TODO Implement this generically somehow. With macro? Maybe a function-like macro that
        //   takes a lot of implementations of forward where the arguments have concrete types and
        //   outputs one big forward that does the appropriate matching and casting
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &BatterIdExtrapolated = extrapolated.try_into().unwrap();
            game.team_at_bat_mut().batter = extrapolated.batter_id;

            self.game_update.forward(game);

            let prev_batter_count = game.team_at_bat().team_batter_count
                .expect("TeamBatterCount must be populated during a game");
            game.team_at_bat_mut().team_batter_count = Some(prev_batter_count + 1);
            game.team_at_bat_mut().batter_name = Some(self.batter_name.clone());
        }
        entity
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

impl Display for BatterUp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatterUp for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(BatterUp);