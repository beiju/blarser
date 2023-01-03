use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use as_any::Downcast;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::entity::AnyEntity;
use crate::events::{AnyExtrapolated, Effect, Event};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct TogglePerforming {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) player_id: Uuid,
    pub(crate) source_mod: String,
    pub(crate) is_overperforming: bool,
}

impl Event for TogglePerforming {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id),
            Effect::one_id(EntityType::Player, self.player_id),
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            self.game_update.forward(game);

            // TODO I think these were misplaced and should just be on HalfInning
            game.game_start = true;
            game.game_start_phase = 10;  // Used to be -1
            game.home.team_batter_count = Some(-1);
            game.away.team_batter_count = Some(-1);
        } else if let Some(player) = entity.as_player_mut() {
            let which_mod = if self.is_overperforming {
                "OVERPERFORMING"
            } else {
                "UNDERPERFORMING"
            };
            player.perm_attr.as_mut()
                .expect("Everyone but Phantom Sixpack has this")
                .push(which_mod.to_string());
            let perm_mod_sources = &mut player.state.as_mut()
                .expect("Everyone but Phantom Sixpack has this")
                .perm_mod_sources;

            if perm_mod_sources.is_none() {
                *perm_mod_sources = Some(HashMap::new());
            }
            perm_mod_sources.as_mut().unwrap()
                .entry(which_mod.to_string()).or_default()
                .push(self.source_mod.clone());
        }
        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        todo!()
    }
}

impl Display for TogglePerforming {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TogglePerforming for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(TogglePerforming);