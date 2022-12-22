use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::{Conflict, MaybeKnown, PartialInformationCompare};

use crate::entity::AnyEntity;
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::PitchersExtrapolated;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct HalfInning {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
}

// fn read_active_pitcher(state: &mut StateInterface, team_id: Uuid, day: i32) -> QueryResult<Vec<(Uuid, String)>> {
//     let result = state.read_team(team_id, |team| {
//         team.active_pitcher(day)
//     })?
//         .into_iter()
//         .map(|pitcher_id| {
//             state.read_player(pitcher_id, |player| {
//                 (pitcher_id, player.name)
//             })
//         })
//         .collect::<Result<Vec<_>, _>>()?
//         .into_iter()
//         .flatten()
//         .collect();
//
//     Ok(result)
// }

impl Event for HalfInning {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id, PitchersExtrapolated::new())
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &PitchersExtrapolated = extrapolated.try_into()
                .expect("Wrong extrapolated type in HalfInning update");

            self.game_update.forward(game);

            if game.inning == -1 {
                game.home.pitcher = Some(extrapolated.home_pitcher_id);
                game.home.pitcher_name = Some(extrapolated.home_pitcher_name.clone());
                game.away.pitcher = Some(extrapolated.away_pitcher_id);
                game.away.pitcher_name = Some(extrapolated.away_pitcher_name.clone());
            }

            game.top_of_inning = !game.top_of_inning;
            if game.top_of_inning {
                game.inning += 1;
            }
            game.phase = 6;
            game.game_start_phase = 10;
            game.half_inning_score = 0.0;
        }

        entity
    }

    fn backward(&self, extrapolated: &AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict> {
        let mut conflicts = Vec::new();
        
        if let Some(game) = entity.as_game_mut() {
            game.home.pitcher = None;
            game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
            game.away.pitcher = None;
            game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));
        }

        conflicts
    }
}

impl Display for HalfInning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HalfInning for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(HalfInning);