use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::Conflict;

use crate::entity::{AnyEntity, Base};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::BatterIdExtrapolated;
use crate::events::event_util::game_effect_with_batter_id;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;


#[derive(Debug, Serialize, Deserialize)]
pub struct Walk {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Walk {
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

            let batter_id = *game.team_at_bat().batter.as_ref()
                .expect("Batter must exist during Walk event"); // not sure why clone works and not * for a Copy type but whatever
            let batter_name = game.team_at_bat().batter_name.clone()
                .expect("Batter name must exist during Walk event");

            // game.advance_runners(&advancements);
            let batter_mod = game.team_at_bat().batter_mod.clone();
            game.push_base_runner(batter_id, batter_name.clone(), batter_mod, Base::First);
            game.end_at_bat();
        }
        entity
    }

    fn backward(&self, _extrapolated: &AnyExtrapolated, _entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for Walk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Walk for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(Walk);
