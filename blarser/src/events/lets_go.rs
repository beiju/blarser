use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::events::{Effect, Event, Extrapolated, ord_by_time};
use crate::events::game_update::GameUpdate;
use crate::state::EntityType;

#[derive(Serialize, Deserialize)]
pub struct LetsGo {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for LetsGo {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id),
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &Box<dyn Extrapolated>) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            self.game_update.forward(game);

            game.game_start = true;
            game.game_start_phase = -1;
            game.home.team_batter_count = Some(-1);
            game.away.team_batter_count = Some(-1);
        }
        entity
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

impl Display for LetsGo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LetsGo for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(LetsGo);