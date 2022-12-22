use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::Conflict;

use crate::entity::AnyEntity;
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::BatterIdExtrapolated;
use crate::events::event_util::game_effect_with_batter_id;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;

#[derive(Debug, Serialize, Deserialize)]
pub struct Strike {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Strike {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            game_effect_with_batter_id(self.game_update.game_id, state)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &BatterIdExtrapolated = extrapolated.try_into().unwrap();
            game.team_at_bat_mut().batter = extrapolated.batter_id;

            self.game_update.forward(game);

            game.at_bat_strikes += 1;
        }
        entity
    }

    fn backward(&self, _extrapolated: &AnyExtrapolated, _entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for Strike {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Strike for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(Strike);

#[derive(Debug, Serialize, Deserialize)]
pub struct Ball {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Ball {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            game_effect_with_batter_id(self.game_update.game_id, state)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &BatterIdExtrapolated = extrapolated.try_into().unwrap();
            game.team_at_bat_mut().batter = extrapolated.batter_id;

            self.game_update.forward(game);

            game.at_bat_balls += 1;
        }
        entity
    }

    fn backward(&self, _extrapolated: &AnyExtrapolated, _entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for Ball {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ball for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(Ball);

#[derive(Debug, Serialize, Deserialize)]
pub struct FoulBall {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for FoulBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            game_effect_with_batter_id(self.game_update.game_id, state)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &BatterIdExtrapolated = extrapolated.try_into().unwrap();
            game.team_at_bat_mut().batter = extrapolated.batter_id;

            self.game_update.forward(game);

            let strikes_to_strike_out = game.team_at_bat().strikes
                .expect("{home/away}Strikes must be set during FoulBall event");
            if game.at_bat_strikes + 1 < strikes_to_strike_out {
                game.at_bat_strikes += 1;
            }
        }
        entity
    }

    fn backward(&self, _extrapolated: &AnyExtrapolated, _entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for FoulBall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FoulBall for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(FoulBall);
