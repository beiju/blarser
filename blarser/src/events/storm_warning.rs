use std::fmt::{Display, Formatter};
use std::sync::Arc;
use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{Effect, AnyEvent, Event, ord_by_time, Extrapolated, AnyExtrapolated};
use crate::events::game_update::GameUpdate;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct StormWarning {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for StormWarning {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
        todo!()
    }

    // fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
    //     match entity {
    //         AnyEntity::Game(mut game) => {
    //             self.game_update.forward(&mut game);
    //
    //             game.game_start_phase = 11; // i guess
    //
    //             game.into()
    //         },
    //         other => panic!("StormWarning event does not apply to {}", other.name())        }
    // }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

impl Display for StormWarning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StormWarning for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(StormWarning);