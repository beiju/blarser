use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use log::info;
use serde::{Deserialize, Serialize};

use crate::entity::{AnyEntity, Base};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::{GamePlayerExtrapolated, NullExtrapolated};
use crate::events::event_util::{game_effect_with_batter, game_effect_with_new_runner};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct Hit {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) to_base: Base,
}

impl Event for Hit {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, state: &StateGraph) -> Vec<Effect> {
        vec![
            game_effect_with_new_runner(self.game_update.game_id, state)
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &GamePlayerExtrapolated = extrapolated.try_into().unwrap();

            self.game_update.forward(game);

            let batter_id = *game.team_at_bat().batter.as_ref()
                .expect("Batter must exist during Hit event"); // not sure why clone works and not * for a Copy type but whatever
            assert_eq!(batter_id, extrapolated.player_id);
            let batter_name = game.team_at_bat().batter_name.clone()
                .expect("Batter name must exist during Hit event");

            // game.advance_runners(&advancements);
            let batter_mod = extrapolated.player_mod.clone();
            info!("In Hit event pushing baserunner {batter_id} ({batter_name}) with mod \"{batter_mod}\"");
            game.push_base_runner(batter_id, batter_name.clone(), batter_mod, self.to_base);
            game.end_at_bat();
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for Hit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hit for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(Hit);

#[derive(Debug, Serialize, Deserialize)]
pub struct HomeRun {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) num_runs: i32,
}

impl Event for HomeRun {
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

            // game_update usually takes care of the scoring but home runs are weird
            game.score_update = Some(format!("1 Run scored!")); // TODO other run numbers
            game.top_inning_score += self.num_runs as f32;
            game.half_inning_score += self.num_runs as f32;
            *game.team_at_bat_mut().score.as_mut().unwrap() += self.num_runs as f32;

            game.clear_bases();
            game.end_at_bat();
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