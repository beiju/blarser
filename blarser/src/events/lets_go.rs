use chrono::{DateTime, Utc};
use diesel::QueryResult;
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::Entity;
use crate::events::{Event, EventAux, EventTrait};

pub struct LetsGo {
    time: DateTime<Utc>
}

impl LetsGo {
    pub fn parse(feed_event: EventuallyEvent) -> QueryResult<(Event, Vec<(String, Uuid, EventAux)>)> {
        let event = Self {
            time: feed_event.created
        };

        let effects = vec![(
            "game".to_string(),
            feed_event.game_id().expect("LetsGo event must have a game id"),
            EventAux::None
        )];

        Ok((event.into(), effects))
    }
}

impl EventTrait for LetsGo {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: Entity, aux: &EventAux) -> Entity {
        todo!()
    }

    fn reverse(&self, entity: Entity, aux: &EventAux) -> Entity {
        todo!()
    }
}