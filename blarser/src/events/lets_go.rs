use chrono::{DateTime, Utc};
use diesel::QueryResult;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::api::EventuallyEvent;
use crate::entity::Entity;
use crate::events::{Event, EventAux, EventTrait};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct LetsGo {
    game_update: GameUpdate,
    time: DateTime<Utc>,
}

impl LetsGo {
    pub fn parse(feed_event: EventuallyEvent) -> QueryResult<(Event, Vec<(String, Option<Uuid>, EventAux)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("LetsGo event must have a game id");
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

impl EventTrait for LetsGo {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: Entity, _: &EventAux) -> Entity {
        match entity {
            Entity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.game_start_phase = 20;
                game.inning = -1;
                game.phase = 2;
                game.top_of_inning = false;

                // Yeah, it unsets pitchers. Why, blaseball.
                game.home.pitcher = None;
                game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
                game.away.pitcher = None;
                game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));

                game.into()
            },
            _ => panic!("LetsGo event does not apply to this entity")
        }
    }

    fn reverse(&self, _entity: Entity, _aux: &EventAux) -> Entity {
        todo!()
    }
}