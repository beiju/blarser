use std::fmt::{Display, Formatter};
use std::iter;
use chrono::{DateTime, Utc};
use itertools::zip_eq;
use log::info;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{MaybeKnown, PartialInformationCompare, RangeInclusive};

use crate::entity::{AnyEntity, Base, Game, Player};
use crate::events::{AnyExtrapolated, Effect, Event};
use crate::events::effects::{AdvancementExtrapolated, DisplayedModChangeExtrapolated, HitExtrapolated, NullExtrapolated};
use crate::events::event_util::{get_displayed_mod_excluding, new_runner_extrapolated, PITCHER_MOD_PRECEDENCE, RUNNER_MOD_PRECEDENCE};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct Hit {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) batter_id: Uuid,
    pub(crate) to_base: Base,
}

impl Event for Hit {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        let num_occupied_bases = state.query_game_unique(self.game_update.game_id, |game| {
            game.bases_occupied.len()
        });
        
        let scores = self.game_update.scores.as_ref()
            .expect("Hit type always has a Scores");

        vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id, HitExtrapolated {
                runner: new_runner_extrapolated(self.game_update.game_id, state),
                advancements: AdvancementExtrapolated::new(num_occupied_bases),
                mod_changes: DisplayedModChangeExtrapolated::new(self.game_update.game_id, &scores.free_refills, state),
            }),
            Effect::one_id(EntityType::Player, self.batter_id),
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &HitExtrapolated = extrapolated.try_into().unwrap();

            let batter_id = *game.team_at_bat().batter.as_ref()
                .expect("Batter must exist during Hit event"); // not sure why clone works and not * for a Copy type but whatever
            assert_eq!(batter_id, extrapolated.runner.player_id);
            let batter_name = game.team_at_bat().batter_name.clone()
                .expect("Batter name must exist during Hit event");

            // game.advance_runners(&advancements);
            let batter_mod = extrapolated.runner.player_mod.clone();
            info!("In Hit event pushing baserunner {batter_id} ({batter_name}) with mod \"{batter_mod}\"");
            game.advance_runners_by(self.to_base as i32 + 1);
            for (base_occupied, advanced) in zip_eq(&mut game.bases_occupied, &extrapolated.advancements.bases) {
                base_occupied.maybe_add(advanced, 1);
            }
            game.push_base_runner(batter_id, batter_name.clone(), batter_mod, self.to_base);
            game.end_at_bat();

            extrapolated.mod_changes.forward(game);

            self.game_update.forward(game);
        } else if let Some(player) = entity.as_player_mut() {
            let _: &NullExtrapolated = extrapolated.try_into()
                .expect("Mismatched extrapolated type");

            *player.consecutive_hits.as_mut()
                .expect("Everyone but phantom sixpack has this") += 1;
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let extrapolated: &mut HitExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                self.game_update.reverse(old_game, new_game);
                new_game.reverse_end_at_bat(old_game);
                new_game.reverse_push_base_runner();
                new_game.advance_runners_by(-(self.to_base as i32 + 1));
                
                extrapolated.mod_changes.reverse(old_game, new_game);

                for ((new_base_occupied, advanced), old_base_occupied) in zip_eq(zip_eq(&mut new_game.bases_occupied, &mut extrapolated.advancements.bases), &old_game.bases_occupied) {
                    if old_base_occupied.upper < new_base_occupied.lower {
                        // If the new base range doesn't overlap with the old base range, we know
                        // they advanced and that they must have been at the upper end of the old
                        // base range.
                        *advanced = MaybeKnown::Known(true);
                        *new_base_occupied = RangeInclusive::from_raw(old_base_occupied.upper);
                    } else if old_base_occupied.lower == new_base_occupied.upper {
                        // If the new base range ends where the old base range starts, we know they
                        // can't have advanced and that they must have been at the lower end of the
                        // old ase range
                        *advanced = MaybeKnown::Known(false);
                        *new_base_occupied = RangeInclusive::from_raw(old_base_occupied.lower);
                    } else {
                        // Otherwise we know nothing
                        *new_base_occupied = *old_base_occupied;
                    }
                }
            }
            AnyEntity::Player(_old_player) => {
                let new_player: &mut Player = new_parent.try_into()
                    .expect("Mismatched entity type");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");

                *new_player.consecutive_hits.as_mut()
                    .expect("Everyone but phantom sixpack has this") -= 1;
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for Hit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hit for {} at {}", self.game_update.game_id, self.time)
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct HomeRun {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) batter_id: Uuid,
    pub(crate) num_runs: i32,
}

impl Event for HomeRun {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id),
            Effect::one_id(EntityType::Player, self.batter_id),
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let _: &NullExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);

            // game_update usually takes care of the scoring but home runs are weird
            game.score_update = Some(match self.num_runs {
                1 => format!("1 Run scored!"),
                x => format!("{x} Runs scored!"),
            });
            *game.current_half_score_mut() += self.num_runs as f32;
            game.half_inning_score += self.num_runs as f32;
            *game.team_at_bat_mut().score.as_mut().unwrap() += self.num_runs as f32;

            game.clear_bases();
            game.end_at_bat();
        } else if let Some(player) = entity.as_player_mut() {
            let _: &NullExtrapolated = extrapolated.try_into()
                .expect("Mismatched extrapolated type");

            *player.consecutive_hits.as_mut()
                .expect("Everyone but phantom sixpack has this") += 1;
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for HomeRun {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HomeRun for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(HomeRun);