use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct StormWarning {
    game_update: GameUpdate,
    time: DateTime<Utc>,
}

impl StormWarning {
    pub fn parse(feed_event: EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("StormWarning event must have a game id");
        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::StormWarning(event), effects))
    }
}

impl Event for StormWarning {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.game_start_phase = 11; // i guess

                game.into()
            },
            other => panic!("StormWarning event does not apply to {}", other.name())        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}