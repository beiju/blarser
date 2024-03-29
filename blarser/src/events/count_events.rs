use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::entity::AnyEntity;
use crate::events::{AnyExtrapolated, Effect, Event};
use crate::events::effects::{GamePlayerExtrapolated, NullExtrapolated};
use crate::events::event_util::game_effect_with_batter;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct Strike {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Strike {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![Effect::one_id(EntityType::Game, self.game_update.game_id)]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);

            game.at_bat_strikes += 1;
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for Strike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Strike for {} at {}", self.game_update.game_id, self.time)
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Ball {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Ball {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![Effect::one_id(EntityType::Game, self.game_update.game_id)]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);

            game.at_bat_balls += 1;
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for Ball {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ball for {} at {}", self.game_update.game_id, self.time)
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct FoulBall {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for FoulBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![Effect::one_id(EntityType::Game, self.game_update.game_id)]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);

            let strikes_to_strike_out = game.team_at_bat().strikes
                .expect("{home/away}Strikes must be set during FoulBall event");
            if game.at_bat_strikes + 1 < strikes_to_strike_out {
                game.at_bat_strikes += 1;
            }
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for FoulBall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FoulBall for {} at {}", self.game_update.game_id, self.time)
    }
}

