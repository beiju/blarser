// Entity types
mod player;
mod sim;
mod game;
mod team;
mod standings;
mod season;
mod common;

use std::fmt::{Display, Formatter};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use partial_information::{Conflict, PartialInformationCompare};

// use crate::events::AnyEvent;

pub use common::{Base, RunnerAdvancement};
pub use sim::Sim;
pub use player::Player;
pub use team::Team;
pub use game::{Game, GameByTeam, UpdateFull, UpdateFullMetadata};
use partial_information_derive::PartialInformationCompare;
pub use standings::Standings;
pub use season::Season;

#[enum_dispatch]
pub trait Entity: Serialize + for<'de> Deserialize<'de> + PartialEq + Clone + Display {
    fn entity_type(self) -> &'static str;
    fn id(&self) -> Uuid;
}

#[derive(Debug)]
pub struct WrongEntityError {
    expected: &'static str,
    found: &'static str,
}

impl Display for WrongEntityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expected {} but found {}", self.expected, self.found)
    }
}

#[enum_dispatch(Entity)]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum AnyEntity {
    Sim(Sim),
    Player(Player),
    Team(Team),
    Game(Game),
    Standings(Standings),
    Season(Season),
}

impl Display for AnyEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyEntity::Sim(e) => { e.fmt(f) }
            AnyEntity::Player(e) => { e.fmt(f) }
            AnyEntity::Team(e) => { e.fmt(f) }
            AnyEntity::Game(e) => { e.fmt(f) }
            AnyEntity::Standings(e) => { e.fmt(f) }
            AnyEntity::Season(e) => { e.fmt(f) }
        }
    }
}

impl AnyEntity {
    // pub fn raw_from_json(entity_type: &str, json: serde_json::Value) -> Result<Self, EntityParseError> {
    //     Ok(match entity_type {
    //         "sim" => Self::Sim(serde_json::from_value(json)?),
    //         "player" => Self::Player(serde_json::from_value(json)?),
    //         "team" => Self::Team(serde_json::from_value(json)?),
    //         "game" => Self::Game(serde_json::from_value(json)?),
    //         "standings" => Self::Standings(serde_json::from_value(json)?),
    //         "season" => Self::Season(serde_json::from_value(json)?),
    //         other => return Err(EntityParseError::UnknownEntity(other.to_string())),
    //     })
    // }

}

pub trait EntityRaw: Serialize + for<'de> Deserialize<'de> {
    type Entity: Entity + PartialInformationCompare<Raw=Self> + Serialize + for<'de> Deserialize<'de>;

    fn name() -> &'static str;
    fn id(&self) -> Uuid;

    // By default an entity doesn't have any init events
    // fn init_events(&self, _after_time: DateTime<Utc>) -> Vec<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
    //     Vec::new()
    // }
}

#[derive(Debug, Error)]
pub enum EntityParseError {
    #[error("Unknown entity type {0}")]
    UnknownEntity(String),

    #[error(transparent)]
    DeserializeFailed(#[from] serde_json::Error),
}