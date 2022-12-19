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
use derive_more::{From, TryInto, Unwrap};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use partial_information::{Conflict, PartialInformationCompare};

// use crate::events::AnyEvent;

pub use common::{Base, RunnerAdvancement};
pub use sim::Sim;
pub use player::Player;
pub use team::Team;
pub use game::{Game, GameByTeam, UpdateFull, UpdateFullMetadata};
pub use standings::Standings;
pub use season::Season;
use crate::state::EntityType;

pub trait Entity: Serialize + for<'de> Deserialize<'de> + PartialEq + Clone + Display {
    fn entity_type(&self) -> EntityType;
    fn id(&self) -> Uuid;

    fn description(&self) -> String;
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

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, From, TryInto, Unwrap)]
#[try_into(owned, ref, ref_mut)]
pub enum AnyEntity {
    Sim(Sim),
    Player(Player),
    Team(Team),
    Game(Game),
    Standings(Standings),
    Season(Season),
}

macro_rules! impl_match {
    ($any_entity_var:expr, $pattern_var:ident => $pattern_block:block) => {
        match $any_entity_var {
            AnyEntity::Sim($pattern_var) => $pattern_block
            AnyEntity::Player($pattern_var) => $pattern_block
            AnyEntity::Team($pattern_var) => $pattern_block
            AnyEntity::Game($pattern_var) => $pattern_block
            AnyEntity::Standings($pattern_var) => $pattern_block
            AnyEntity::Season($pattern_var) => $pattern_block
        }

    };
}

impl Display for AnyEntity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        impl_match!(self, e => { e.fmt(f) })
    }
}

impl Entity for AnyEntity {
    fn entity_type(&self) -> EntityType {
        impl_match!(&self, e => { e.entity_type() })
    }

    fn id(&self) -> Uuid {
        impl_match!(&self, e => { e.id() })
    }

    fn description(&self) -> String {
        impl_match!(&self, e => { e.description() })
    }
}

macro_rules! impl_as_ref {
    ($entity_type:ty, $entity_variant:path, $ref_name:ident, $mut_name:ident) => {
        pub fn $ref_name(&self) -> Option<&$entity_type> {
            if let $entity_variant(e) = self {
                Some(e)
            } else {
                None
            }
        }

        pub fn $mut_name(&mut self) -> Option<&mut $entity_type> {
            if let $entity_variant(e) = self {
                Some(e)
            } else {
                None
            }
        }
    };
}

impl AnyEntity {
    fn from_raw_json_typed<EntityT>(raw_json: serde_json::Value) -> serde_json::Result<Self>
        where EntityT: Entity + PartialInformationCompare, AnyEntity: From<EntityT> {
        let raw: EntityT::Raw = serde_json::from_value(raw_json)?;
        let entity = EntityT::from_raw(raw);
        Ok(AnyEntity::from(entity))
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

    pub fn from_raw(raw: AnyEntityRaw) -> Self {
        match raw {
            AnyEntityRaw::SimRaw(r) => { AnyEntity::Sim(Sim::from_raw(r)) }
            AnyEntityRaw::PlayerRaw(r) => { AnyEntity::Player(Player::from_raw(r)) }
            AnyEntityRaw::TeamRaw(r) => { AnyEntity::Team(Team::from_raw(r)) }
            AnyEntityRaw::GameRaw(r) => { AnyEntity::Game(Game::from_raw(r)) }
            AnyEntityRaw::StandingsRaw(r) => { AnyEntity::Standings(Standings::from_raw(r)) }
            AnyEntityRaw::SeasonRaw(r) => { AnyEntity::Season(Season::from_raw(r)) }
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        impl_match!(&self, e => { serde_json::to_value(e).unwrap() })
    }

    pub fn is_ambiguous(&self) -> bool {
        impl_match!(&self, e => { e.is_ambiguous() })
    }

    impl_as_ref!(Sim, AnyEntity::Sim, as_sim, as_sim_mut);
    impl_as_ref!(Game, AnyEntity::Game, as_game, as_game_mut);
    impl_as_ref!(Team, AnyEntity::Team, as_team, as_team_mut);
    impl_as_ref!(Player, AnyEntity::Player, as_player, as_player_mut);
}


pub trait EntityRaw: Serialize + for<'de> Deserialize<'de> {
    type Entity: Entity + PartialInformationCompare<Raw=Self> + Serialize + for<'de> Deserialize<'de>;

    fn name() -> &'static str;
    fn id(&self) -> Uuid;
}

#[derive(Debug, Clone, Deserialize, Serialize, From, TryInto, Unwrap)]
#[try_into(owned, ref, ref_mut)]
pub enum AnyEntityRaw {
    SimRaw(<Sim as PartialInformationCompare>::Raw),
    PlayerRaw(<Player as PartialInformationCompare>::Raw),
    TeamRaw(<Team as PartialInformationCompare>::Raw),
    GameRaw(<Game as PartialInformationCompare>::Raw),
    StandingsRaw(<Standings as PartialInformationCompare>::Raw),
    SeasonRaw(<Season as PartialInformationCompare>::Raw),
}

impl AnyEntityRaw {
    fn from_json_typed<EntityT>(json: serde_json::Value) -> serde_json::Result<Self>
        where EntityT: Entity + PartialInformationCompare, AnyEntityRaw: From<EntityT::Raw> {
        let raw: EntityT::Raw = serde_json::from_value(json)?;
        Ok(raw.into())
    }

    pub fn from_json(entity_type: EntityType, json: serde_json::Value) -> serde_json::Result<Self> {
        match entity_type {
            EntityType::Sim => { Self::from_json_typed::<Sim>(json) }
            EntityType::Player => { Self::from_json_typed::<Player>(json) }
            EntityType::Team => { Self::from_json_typed::<Team>(json) }
            EntityType::Game => { Self::from_json_typed::<Game>(json) }
            EntityType::Standings => { Self::from_json_typed::<Standings>(json) }
            EntityType::Season => { Self::from_json_typed::<Season>(json) }
        }
    }

    pub fn to_json(self) -> serde_json::Result<serde_json::Value> {
        match self {
            AnyEntityRaw::SimRaw(r) => { serde_json::to_value(r) }
            AnyEntityRaw::PlayerRaw(r) => { serde_json::to_value(r) }
            AnyEntityRaw::TeamRaw(r) => { serde_json::to_value(r) }
            AnyEntityRaw::GameRaw(r) => { serde_json::to_value(r) }
            AnyEntityRaw::StandingsRaw(r) => { serde_json::to_value(r) }
            AnyEntityRaw::SeasonRaw(r) => { serde_json::to_value(r) }
        }
    }
}

#[derive(Debug, Error)]
pub enum EntityParseError {
    #[error("Unknown entity type {0}")]
    UnknownEntity(String),

    #[error(transparent)]
    DeserializeFailed(#[from] serde_json::Error),
}