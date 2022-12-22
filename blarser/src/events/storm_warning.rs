use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::Conflict;
use crate::entity::AnyEntity;
use crate::events::{Effect, Event, ord_by_time, AnyExtrapolated};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
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

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, _entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
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

    fn backward(&self, _extrapolated: &AnyExtrapolated, _entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for StormWarning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StormWarning for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(StormWarning);