use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use itertools::zip_eq;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare, RangeInclusive};

use crate::entity::{AnyEntity, Base, Game};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::{GamePlayerExtrapolated, NullExtrapolated};
use crate::events::event_util::game_effect_with_batter;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct StolenBase {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) to_base: Base,
    pub(crate) runner_id: Uuid,
}

impl Event for StolenBase {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

            // TODO: If someone else is maybe on this base, make them not be on it
            for (base, id) in zip_eq(&mut game.bases_occupied, game.base_runners.clone()) {
                if id == self.runner_id && base.could_be(&(self.to_base as i32 - 1)) {
                    *base = RangeInclusive::from_raw(self.to_base as i32)
                }
            }

            self.game_update.forward(game);
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                self.game_update.reverse(old_game, new_game);
                for (old_base, new_base) in zip_eq(&mut new_game.bases_occupied, old_game.bases_occupied.clone()) {
                    *old_base = new_base;
                }
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for StolenBase {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StolenBase for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(StolenBase);


#[derive(Debug, Serialize, Deserialize)]
pub struct CaughtStealing {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) to_base: Base,
}

impl Event for CaughtStealing {
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

            let batter_id = *game.team_at_bat().batter.as_ref()
                .expect("Batter must exist during CaughtStealing event"); // not sure why clone works and not * for a Copy type but whatever
            let batter_name = game.team_at_bat().batter_name.clone()
                .expect("Batter name must exist during CaughtStealing event");

            // game.advance_runners(&advancements);
            let batter_mod = game.team_at_bat().batter_mod.clone();
            game.push_base_runner(batter_id, batter_name.clone(), batter_mod, self.to_base);
            game.end_at_bat();
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for CaughtStealing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CaughtStealing for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(CaughtStealing);
