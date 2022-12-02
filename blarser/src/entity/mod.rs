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
use serde::{Deserialize, Serialize};
use thiserror::Error;
use partial_information::PartialInformationCompare;

// use crate::events::AnyEvent;

pub use common::{Base, RunnerAdvancement};
pub use sim::Sim;
pub use player::Player;
pub use team::Team;
pub use game::{Game, GameByTeam, UpdateFull, UpdateFullMetadata};
pub use standings::Standings;
pub use season::Season;

pub trait Entity: PartialInformationCompare + Serialize + for<'de> Deserialize<'de> + Into<AnyEntity> + TryFrom<AnyEntity, Error=WrongEntityError> + PartialEq + Clone + Display {
    fn name() -> &'static str;
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

#[derive(Clone, PartialEq)]
pub enum AnyEntity {
    Sim(Sim),
    Player(Player),
    Team(Team),
    Game(Game),
    Standings(Standings),
    Season(Season),
}

#[macro_export]
macro_rules! with_any_entity {
    ($any_entity:expr, $bound_name:ident => $arm:expr) => {
        match $any_entity {
            crate::entity::AnyEntity::Sim($bound_name) => { $arm }
            crate::entity::AnyEntity::Player($bound_name) => { $arm }
            crate::entity::AnyEntity::Team($bound_name) => { $arm }
            crate::entity::AnyEntity::Game($bound_name) => { $arm }
            crate::entity::AnyEntity::Standings($bound_name) => { $arm }
            crate::entity::AnyEntity::Season($bound_name) => { $arm }
        }
    };
}

pub use with_any_entity;

impl AnyEntity {
    pub fn name(&self) -> &'static str {
        match self {
            AnyEntity::Sim(_) => { Sim::name() }
            AnyEntity::Player(_) => { Player::name() }
            AnyEntity::Team(_) => { Team::name() }
            AnyEntity::Game(_) => { Game::name() }
            AnyEntity::Standings(_) => { Standings::name() }
            AnyEntity::Season(_) => { Season::name() }
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

    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc>;
    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc>;
}

#[derive(Clone)]
pub enum AnyEntityRaw {
    Sim(<Sim as PartialInformationCompare>::Raw),
    Player(<Player as PartialInformationCompare>::Raw),
    Team(<Team as PartialInformationCompare>::Raw),
    Game(<Game as PartialInformationCompare>::Raw),
    Standings(<Standings as PartialInformationCompare>::Raw),
    Season(<Season as PartialInformationCompare>::Raw),
}

#[macro_export]
macro_rules! with_any_entity_raw {
    ($any_entity:expr, $bound_name:ident => $arm:expr) => {
        match $any_entity {
            crate::entity::AnyEntityRaw::Sim($bound_name) => { $arm }
            crate::entity::AnyEntityRaw::Player($bound_name) => { $arm }
            crate::entity::AnyEntityRaw::Team($bound_name) => { $arm }
            crate::entity::AnyEntityRaw::Game($bound_name) => { $arm }
            crate::entity::AnyEntityRaw::Standings($bound_name) => { $arm }
            crate::entity::AnyEntityRaw::Season($bound_name) => { $arm }
        }
    };
}

pub use with_any_entity_raw;

#[derive(Debug, Error)]
pub enum EntityParseError {
    #[error("Unknown entity type {0}")]
    UnknownEntity(String),

    #[error(transparent)]
    DeserializeFailed(#[from] serde_json::Error),
}


impl AnyEntityRaw {
    pub fn from_json(entity_type: &str, json: serde_json::Value) -> Result<Self, EntityParseError> {
        Ok(match entity_type {
            "sim" => Self::Sim(serde_json::from_value(json)?),
            "player" => Self::Player(serde_json::from_value(json)?),
            "team" => Self::Team(serde_json::from_value(json)?),
            "game" => Self::Game(serde_json::from_value(json)?),
            "standings" => Self::Standings(serde_json::from_value(json)?),
            "season" => Self::Season(serde_json::from_value(json)?),
            other => return Err(EntityParseError::UnknownEntity(other.to_string())),
        })
    }

    pub fn name(&self) -> &'static str {
        match self {
            AnyEntityRaw::Sim(_) => { Sim::name() }
            AnyEntityRaw::Player(_) => { Player::name() }
            AnyEntityRaw::Team(_) => { Team::name() }
            AnyEntityRaw::Game(_) => { Game::name() }
            AnyEntityRaw::Standings(_) => { Standings::name() }
            AnyEntityRaw::Season(_) => { Season::name() }
        }
    }

    pub fn id(&self) -> Uuid {
        with_any_entity_raw!(self, raw => raw.id())
    }
}

#[macro_export]
macro_rules! entity_dispatch {
    // The extra-type-parameters form
    ($type_var:expr => $($func:ident).+::<$($extra_type:ty),*>($($args:expr),*); $fallback_pattern:pat => $fallback_arm:expr) => {
        match $type_var {
            "sim" => { $($func).+::<crate::entity::Sim, $($extra_type),*>($($args),*) }
            "game" => { $($func).+::<crate::entity::Game, $($extra_type),*>($($args),*) }
            "team" => { $($func).+::<crate::entity::Team, $($extra_type),*>($($args),*) }
            "player" => { $($func).+::<crate::entity::Player, $($extra_type),*>($($args),*) }
            "standings" => { $($func).+::<crate::entity::Standings, $($extra_type),*>($($args),*) }
            "season" => { $($func).+::<crate::entity::Season, $($extra_type),*>($($args),*) }
            $fallback_pattern => $fallback_arm,
        }
    };
    // The non-.await form
    ($type_var:expr => $($func:ident).+($($args:expr),*); $fallback_pattern:pat => $fallback_arm:expr) => {
        match $type_var {
            "sim" => { $($func).+::<crate::entity::Sim>($($args),*) }
            "game" => { $($func).+::<crate::entity::Game>($($args),*) }
            "team" => { $($func).+::<crate::entity::Team>($($args),*) }
            "player" => { $($func).+::<crate::entity::Player>($($args),*) }
            "standings" => { $($func).+::<crate::entity::Standings>($($args),*) }
            "season" => { $($func).+::<crate::entity::Season>($($args),*) }
            $fallback_pattern => $fallback_arm,
        }
    };
    // The .await form
    ($type_var:expr => $func:ident($($args:expr),*).await; $fallback_pattern:pat => $fallback_arm:expr) => {
        match $type_var {
            "sim" => <$func>::<crate::entity::Sim>($($args),*).await,
            "game" => <$func>::<crate::entity::Game>($($args),*).await,
            "team" => <$func>::<crate::entity::Team>($($args),*).await,
            "player" => <$func>::<crate::entity::Player>($($args),*).await,
            "standings" => <$func>::<crate::entity::Standings>($($args),*).await,
            "season" => <$func>::<crate::entity::Season>($($args),*).await,
            $fallback_pattern => $fallback_arm,
        }
    };
}

pub use entity_dispatch;

fn entity_description_typed<EntityT: Entity>(entity_json: serde_json::Value) -> String {
    let entity: EntityT = serde_json::from_value(entity_json)
        .expect("Failed to deserialize entity");

    entity.to_string()
}

pub fn entity_description(entity_type: &str, entity_json: serde_json::Value) -> String {
    entity_dispatch!(entity_type => entity_description_typed(entity_json);
                     other => panic!("Tried to get entity description for invalid entity {}", other))
}
