mod timed_event;
// Entity types
mod player;
mod sim;
mod game;
mod team;
mod standings;
mod season;

use uuid::Uuid;
use enum_dispatch::enum_dispatch;
use chrono::{DateTime, Utc};
use thiserror::Error;
use partial_information::PartialInformationCompare;

pub use timed_event::{TimedEvent, TimedEventType};
pub use sim::Sim;
pub use player::Player;
pub use team::Team;
pub use game::{Game, GameByTeam};
pub use standings::Standings;
pub use season::Season;

#[enum_dispatch]
pub trait EntityRawTrait {
    fn entity_type(&self) -> &'static str;
    fn entity_id(&self) -> Uuid;

    // By default an entity doesn't have any init events
    fn init_events(&self, after_time: DateTime<Utc>) -> Vec<TimedEvent> {
        Vec::new()
    }

    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc>;
    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc>;

    fn as_entity(self) -> Entity;
}

#[enum_dispatch(EntityRawTrait)]
pub enum EntityRaw {
    Sim(<Sim as PartialInformationCompare>::Raw),
    Player(<Player as PartialInformationCompare>::Raw),
    Team(<Team as PartialInformationCompare>::Raw),
    Game(<Game as PartialInformationCompare>::Raw),
    Standings(<Standings as PartialInformationCompare>::Raw),
    Season(<Season as PartialInformationCompare>::Raw),
}

#[derive(Debug, Error)]
pub enum EntityParseError {
    #[error("Unknown entity type {0}")]
    UnknownEntity(String),

    #[error(transparent)]
    DeserializeFailed(#[from] serde_json::Error),
}


impl EntityRaw {
    pub fn from_json(entity_type: &str, json: serde_json::Value) -> Result<Self, EntityParseError> {
        Ok(match entity_type {
            "entity" => Self::Sim(serde_json::from_value(json)?),
            "player" => Self::Player(serde_json::from_value(json)?),
            "team" => Self::Team(serde_json::from_value(json)?),
            "game" => Self::Game(serde_json::from_value(json)?),
            "standings" => Self::Standings(serde_json::from_value(json)?),
            "season" => Self::Season(serde_json::from_value(json)?),
            other => return Err(EntityParseError::UnknownEntity(other.to_string())),
        })
    }
}

#[enum_dispatch]
pub trait EntityTrait {
    fn entity_type(&self) -> &'static str;
    fn entity_id(&self) -> Uuid;
}

#[derive(PartialEq, Eq)]
#[enum_dispatch(EntityTrait)]
pub enum Entity {
    Sim(Sim),
    Player(Player),
    Team(Team),
    Game(Game),
    Standings(Standings),
    Season(Season),
}

// I would have enum_dispatch do this but I can't figure out the constraints for that
impl Entity {
    pub fn to_json(self) -> serde_json::Value {
        (match self {
            Entity::Sim(internal) => { serde_json::to_value(internal) }
            Entity::Player(internal) => { serde_json::to_value(internal) }
            Entity::Team(internal) => { serde_json::to_value(internal) }
            Entity::Game(internal) => { serde_json::to_value(internal) }
            Entity::Standings(internal) => { serde_json::to_value(internal) }
            Entity::Season(internal) => { serde_json::to_value(internal) }
        }).expect("Error serializing entity")
    }
}