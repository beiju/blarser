use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct PlayBall {
    game_update: GameUpdate,
    time: DateTime<Utc>,
}

impl PlayBall {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("PlayBall event must have a game id");

        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::PlayBall(event), effects))
    }
}

impl Event for PlayBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.game_start_phase = 20;
                game.inning = -1;
                game.phase = 2;
                game.top_of_inning = false;

                // It unsets pitchers :(
                game.home.pitcher = None;
                game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
                game.away.pitcher = None;
                game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));

                game.into()
            },
            other => panic!("PlayBall event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}