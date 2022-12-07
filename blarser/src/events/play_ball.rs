use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::api::EventuallyEvent;
use crate::entity::{AnyEntity, Entity};
use crate::events::{Effect, AnyEvent, Event, ord_by_time, Extrapolated};
use crate::events::game_update::GameUpdate;
use crate::state::EntityType;

#[derive(Serialize, Deserialize)]
pub struct PlayBall {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for PlayBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &Box<dyn Extrapolated>) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            // self.game_update.forward(&mut game);

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

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

impl Display for PlayBall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlayBall for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(PlayBall);