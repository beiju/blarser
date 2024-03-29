use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::{AnyEntity, Base};
use crate::events::{AnyExtrapolated, Effect, Event};
use crate::events::effects::{GamePlayerExtrapolated, NullExtrapolated};
use crate::events::event_util::game_effect_with_batter;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct Walk {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Walk {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
            vec![Effect::one_id(EntityType::Game, self.game_update.game_id)]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

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

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for Walk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Walk for {} at {}", self.game_update.game_id, self.time)
    }
}

