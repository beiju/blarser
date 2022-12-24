use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::{AnyEntity, Base, Game};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::{GamePlayerExtrapolated, NullExtrapolated};
use crate::events::event_util::game_effect_with_batter;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct InningEnd {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for InningEnd {
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

            game.phase = 2;
            self.game_update.forward(game);
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                new_game.phase = old_game.phase;
                self.game_update.reverse(old_game, new_game);
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for InningEnd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InningEnd for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(InningEnd);
