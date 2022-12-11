use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::{Conflict, MaybeKnown};

use crate::entity::{AnyEntity, Entity};
use crate::events::{Effect, Event, ord_by_time, AnyExtrapolated, Extrapolated};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct PlayBall {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for PlayBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            self.game_update.forward(game);

            game.game_start_phase = 20;
            game.inning = -1;
            game.phase = 2;
            game.top_of_inning = false;

            // It unsets pitchers :(
            game.home.pitcher = None;
            game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
            game.away.pitcher = None;
            game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));
        }

        entity
    }

    fn backward(&self, successor: &AnyEntity, extrapolated: &mut AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for PlayBall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlayBall for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(PlayBall);