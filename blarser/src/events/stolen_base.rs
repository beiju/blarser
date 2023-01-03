use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use fed::{FreeRefill, ScoringPlayer};
use itertools::zip_eq;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{PartialInformationCompare, RangeInclusive};

use crate::entity::{AnyEntity, Base, Game};
use crate::events::{AnyExtrapolated, Effect, Event};
use crate::events::effects::{DisplayedModChangeExtrapolated, NullExtrapolated};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct StolenBase {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) to_base: Base,
    pub(crate) runner_id: Uuid,
    pub(crate) runner_name: String,
    pub(crate) free_refill: Option<FreeRefill>,
}

impl Event for StolenBase {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id,
                                DisplayedModChangeExtrapolated::new(
                                    self.game_update.game_id,
                                    self.free_refill.as_ref()
                                        .map(core::slice::from_ref)
                                        .unwrap_or_default(),
                                    state))
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &DisplayedModChangeExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);

            // Pretending fifth base doesn't exist
            if self.to_base == Base::Fourth {
                // score also pops the runner
                GameUpdate::score(game,
                                  &[ScoringPlayer {
                                      player_id: self.runner_id,
                                      player_name: self.runner_name.clone(),
                                      item_damage: None
                                  }],
                                  self.free_refill.as_ref()
                                      .map(core::slice::from_ref)
                                      .unwrap_or_default());
                extrapolated.forward(game);
            } else {
                // TODO: If someone else is maybe on this base, make them not be on it
                for (base, id) in zip_eq(&mut game.bases_occupied, game.base_runners.clone()) {
                    if id == self.runner_id && base.could_be(&(self.to_base as i32 - 1)) {
                        *base = RangeInclusive::from_raw(self.to_base as i32);
                    }
                }
            }
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let extrapolated: &mut DisplayedModChangeExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                self.game_update.reverse(old_game, new_game);
                if self.to_base == Base::Fourth {
                    extrapolated.reverse(old_game, new_game);
                    // what the hell is this formatting. the auto formatter insists on it
                    GameUpdate::reverse_score(old_game, new_game, &[ ScoringPlayer {
                        player_id: self.runner_id,
                        player_name: self.runner_name.clone(),
                        item_damage: None,
                    }],
                                              self.free_refill.as_ref()
                                                  .map(core::slice::from_ref)
                                                  .unwrap_or_default());
                } else {
                    for (old_base, new_base) in zip_eq(&mut new_game.bases_occupied, old_game.bases_occupied.clone()) {
                        *old_base = new_base;
                    }
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

            // TODO Remove the runner

            game.out(1);

            // This is how the game allows the current batter to have another PA at the start of
            // the next inning
            let team_batter_count = game.team_at_bat_mut().team_batter_count.as_mut()
                .expect("team_batter_count must exist during a StolenBase event");
            *team_batter_count -= 1;
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

                let team_batter_count = new_game.team_at_bat_mut().team_batter_count.as_mut()
                    .expect("team_batter_count must exist during a StolenBase event");
                *team_batter_count += 1;

                new_game.reverse_out(1, old_game);

                // TODO Reverse removing the runner

                self.game_update.reverse(old_game, new_game);
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for CaughtStealing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CaughtStealing for {} at {}", self.game_update.game_id, self.time)
    }
}

