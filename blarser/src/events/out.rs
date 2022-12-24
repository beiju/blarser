use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use itertools::zip_eq;
use serde::{Deserialize, Serialize};
use partial_information::{MaybeKnown, PartialInformationCompare};

use crate::entity::{AnyEntity, Game};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::{AdvancementExtrapolated, NullExtrapolated};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct CaughtOut {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for CaughtOut {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        let num_occupied_bases = state.query_game_unique(self.game_update.game_id, |game| {
            game.bases_occupied.len()
        });

        vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id, AdvancementExtrapolated::new(num_occupied_bases))
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &AdvancementExtrapolated = extrapolated.try_into().unwrap();

            for (base_occupied, advanced) in zip_eq(&mut game.bases_occupied, &extrapolated.bases) {
                base_occupied.maybe_add(advanced, 1);
            }
            game.out(1);
            self.game_update.forward(game);
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let extrapolated: &mut AdvancementExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                new_game.reverse_out(1, old_game);

                // Can't do anything if the bases were cleared
                if !new_game.bases_occupied.is_empty() {
                    for ((new_base_occupied, advanced), old_base_occupied) in zip_eq(zip_eq(&mut new_game.bases_occupied, &mut extrapolated.bases), &old_game.bases_occupied) {
                        if !new_base_occupied.is_ambiguous() {
                            if !old_base_occupied.is_ambiguous() {
                                *advanced = MaybeKnown::Known(new_base_occupied.raw_approximation() != old_base_occupied.raw_approximation())
                            } else {
                                todo!()
                            }
                        } else {
                            todo!()
                        }
                        *new_base_occupied = *old_base_occupied;
                    }
                }

                self.game_update.reverse(old_game, new_game);
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for CaughtOut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CaughtOut for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(CaughtOut);

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldersChoice {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for FieldersChoice {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        let num_occupied_bases = state.query_game_unique(self.game_update.game_id, |game| {
            game.bases_occupied.len()
        });

        vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id, AdvancementExtrapolated::new(num_occupied_bases))
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &AdvancementExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);
            for (base_occupied, advanced) in zip_eq(&mut game.bases_occupied, &extrapolated.bases) {
                base_occupied.maybe_add(advanced, 1);
            }
            game.out(1);
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let extrapolated: &mut AdvancementExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                new_game.reverse_out(1, old_game);

                // Can't do anything if the bases were cleared
                if !new_game.bases_occupied.is_empty() {
                    for ((new_base_occupied, advanced), old_base_occupied) in zip_eq(zip_eq(&mut new_game.bases_occupied, &mut extrapolated.bases), &old_game.bases_occupied) {
                        if !new_base_occupied.is_ambiguous() {
                            if !old_base_occupied.is_ambiguous() {
                                *advanced = MaybeKnown::Known(new_base_occupied.raw_approximation() != old_base_occupied.raw_approximation())
                            } else {
                                todo!()
                            }
                        } else {
                            todo!()
                        }
                        *new_base_occupied = *old_base_occupied;
                    }
                }

                self.game_update.reverse(old_game, new_game);
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for FieldersChoice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FieldersChoice for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(FieldersChoice);

#[derive(Debug, Serialize, Deserialize)]
pub struct Strikeout {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

impl Event for Strikeout {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);
            game.out(1);
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

                new_game.reverse_out(1, old_game);
                self.game_update.reverse(old_game, new_game);
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for Strikeout {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Strikeout for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(Strikeout);