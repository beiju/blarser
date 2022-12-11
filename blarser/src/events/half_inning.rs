use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use partial_information::Conflict;

use crate::entity::{AnyEntity, Entity};
use crate::events::{AnyExtrapolated, Effect, Event, Extrapolated, ord_by_time};
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
            Effect::one_id(EntityType::Game, self.game_update.game_id)
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            self.game_update.forward(game);

            game.top_of_inning = !game.top_of_inning;
            if game.top_of_inning {
                game.inning += 1;
            }
            game.phase = 6;
            game.half_inning_score = 0.0;

            // The first halfInning event re-sets the data that PlayBall clears
            // if let Some(starting_pitchers) = aux {
            //     let (home_pitcher, home_pitcher_name) = starting_pitchers.home;
            //     let (away_pitcher, away_pitcher_name) = starting_pitchers.away;
            //
            //     game.home.pitcher = Some(MaybeKnown::Known(home_pitcher));
            //     game.home.pitcher_name = Some(MaybeKnown::Known(home_pitcher_name));
            //     game.away.pitcher = Some(MaybeKnown::Known(away_pitcher));
            //     game.away.pitcher_name = Some(MaybeKnown::Known(away_pitcher_name));
            // }
        }

        entity
    }

    fn backward(&self, successor: &AnyEntity, extrapolated: &mut AnyExtrapolated, entity: &mut AnyEntity) -> Vec<Conflict> {
        todo!()
    }
}

impl Display for HalfInning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HalfInning for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(HalfInning);