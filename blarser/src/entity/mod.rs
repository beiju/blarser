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
use crate::state::EntityType;

#[enum_dispatch]
pub trait Entity: Serialize + for<'de> Deserialize<'de> + PartialEq + Clone + Display {
    fn entity_type(&self) -> &'static str;
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
enum AnyEntityInternal {
    Sim(Sim),
    Player(Player),
    Team(Team),
    Game(Game),
    Standings(Standings),
    Season(Season),
}

impl Display for AnyEntityInternal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyEntityInternal::Sim(e) => { e.fmt(f) }
            AnyEntityInternal::Player(e) => { e.fmt(f) }
            AnyEntityInternal::Team(e) => { e.fmt(f) }
            AnyEntityInternal::Game(e) => { e.fmt(f) }
            AnyEntityInternal::Standings(e) => { e.fmt(f) }
            AnyEntityInternal::Season(e) => { e.fmt(f) }
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct AnyEntity(AnyEntityInternal);

impl Display for AnyEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            AnyEntityInternal::Sim(e) => { e.fmt(f) }
            AnyEntityInternal::Player(e) => { e.fmt(f) }
            AnyEntityInternal::Team(e) => { e.fmt(f) }
            AnyEntityInternal::Game(e) => { e.fmt(f) }
            AnyEntityInternal::Standings(e) => { e.fmt(f) }
            AnyEntityInternal::Season(e) => { e.fmt(f) }
        }
    }
}

impl Entity for AnyEntity {
    fn entity_type(&self) -> &'static str {
        match &self.0 {
            AnyEntityInternal::Sim(e) => { e.entity_type() }
            AnyEntityInternal::Player(e) => { e.entity_type() }
            AnyEntityInternal::Team(e) => { e.entity_type() }
            AnyEntityInternal::Game(e) => { e.entity_type() }
            AnyEntityInternal::Standings(e) => { e.entity_type() }
            AnyEntityInternal::Season(e) => { e.entity_type() }
        }
    }

    fn id(&self) -> Uuid {
        match &self.0 {
            AnyEntityInternal::Sim(e) => { e.id() }
            AnyEntityInternal::Player(e) => { e.id() }
            AnyEntityInternal::Team(e) => { e.id() }
            AnyEntityInternal::Game(e) => { e.id() }
            AnyEntityInternal::Standings(e) => { e.id() }
            AnyEntityInternal::Season(e) => { e.id() }
        }
    }
}

impl AnyEntity {
    fn from_raw_json_typed<EntityT>(raw_json: serde_json::Value) -> serde_json::Result<Self>
        where EntityT: Entity + PartialInformationCompare, AnyEntityInternal: From<EntityT> {
        let raw: EntityT::Raw = serde_json::from_value(raw_json)?;
        let entity = EntityT::from_raw(raw);
        Ok(AnyEntity(AnyEntityInternal::from(entity)))
    }

    pub fn from_raw_json(entity_type: EntityType, raw_json: serde_json::Value) -> serde_json::Result<Self> {
        match entity_type {
            EntityType::Sim => { Self::from_raw_json_typed::<Sim>(raw_json) }
            EntityType::Player => { Self::from_raw_json_typed::<Player>(raw_json) }
            EntityType::Team => { Self::from_raw_json_typed::<Team>(raw_json) }
            EntityType::Game => { Self::from_raw_json_typed::<Game>(raw_json) }
            EntityType::Standings => { Self::from_raw_json_typed::<Standings>(raw_json) }
            EntityType::Season => { Self::from_raw_json_typed::<Season>(raw_json) }
        }
    }

    pub fn as_sim(&self) -> Option<&Sim> {
        if let AnyEntityInternal::Sim(sim) = &self.0 {
            Some(sim)
        } else {
            None
        }
    }
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