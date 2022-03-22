use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::Entity;
use crate::events::{Event, EventAux, EventTrait};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct PlayBall {
    game_update: GameUpdate,
    time: DateTime<Utc>,
}

impl PlayBall {
    pub fn parse(feed_event: EventuallyEvent) -> QueryResult<(Event, Vec<(String, Option<Uuid>, EventAux)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("PlayBall event must have a game id");

        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            EventAux::None
        )];

        Ok((event.into(), effects))
    }
}

impl EventTrait for PlayBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: Entity, _: &EventAux) -> Entity {
        match entity {
            Entity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.game_start = true;
                game.game_start_phase = -1;
                game.home.team_batter_count = Some(-1);
                game.away.team_batter_count = Some(-1);

                game.into()
            },
            _ => panic!("PlayBall event does not apply to this entity")
        }
    }

    fn reverse(&self, _entity: Entity, _aux: &EventAux) -> Entity {
        todo!()
    }
}