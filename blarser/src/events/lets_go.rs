use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct LetsGo {
    game_update: GameUpdate,
    time: DateTime<Utc>,
}

impl LetsGo {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("LetsGo event must have a game id");
        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time,
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::LetsGo(event), effects))
    }
}

impl Event for LetsGo {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.game_start = true;
                game.game_start_phase = -1;
                game.home.team_batter_count = Some(-1);
                game.away.team_batter_count = Some(-1);

                game.into()
            }
            other => panic!("LetsGo event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}