use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::entity::{AnyEntity, Game, Team};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::NullExtrapolated;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct LetsGo {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) home_team: Uuid,
    pub(crate) away_team: Uuid,
}

impl Event for LetsGo {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id),
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
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

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched event types");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");

                new_game.game_start = old_game.game_start;
                new_game.game_start_phase = old_game.game_start_phase;
                new_game.home.team_batter_count = old_game.home.team_batter_count;
                new_game.away.team_batter_count = old_game.away.team_batter_count;

                self.game_update.reverse(old_game, new_game);
            }
            _ => {
                panic!("Can't reverse-apply LetsGo to this entity type");
            }
        }
    }
}

impl Display for LetsGo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LetsGo for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(LetsGo);