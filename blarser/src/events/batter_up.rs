use std::any::{Any, TypeId};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use log::info;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;
use partial_information_derive::PartialInformationCompare;
use as_any::Downcast;

use crate::entity::AnyEntity;
use crate::events::{AnyExtrapolated, Effect, Event, Extrapolated, ord_by_time};
use crate::events::game_update::GameUpdate;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct BatterUp {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) batter_name: String,
}

#[derive(Debug, PartialInformationCompare)]
pub struct BatterUpExtrapolated {
    pub(crate) batter_id: MaybeKnown<Uuid>,
}

impl Extrapolated for BatterUpExtrapolated {}

impl Event for BatterUp {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self) -> Vec<Effect> {
        vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id, BatterUpExtrapolated {
                // TODO: Is this available in state?
                batter_id: MaybeKnown::Unknown,
            })
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        // TODO Implement this generically somehow. With macro? Maybe a function-like macro that
        //   takes a lot of implementations of forward where the arguments have concrete types and
        //   outputs one big forward that does the appropriate matching and casting
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            dbg!(extrapolated);
            dbg!(extrapolated.type_id());
            dbg!(TypeId::of::<BatterUpExtrapolated>());
            dbg!(TypeId::of::<&BatterUpExtrapolated>());
            let extrapolated = extrapolated.downcast_ref::<BatterUpExtrapolated>().unwrap();
            // self.game_update.forward(&mut game);

            let prev_batter_count = game.team_at_bat().team_batter_count
                .expect("TeamBatterCount must be populated during a game");
            game.team_at_bat_mut().team_batter_count = Some(prev_batter_count + 1);
            game.team_at_bat_mut().batter = Some(extrapolated.batter_id.clone());
            game.team_at_bat_mut().batter_name = Some(self.batter_name.clone());
        }
        entity
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

impl Display for BatterUp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatterUp for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(BatterUp);