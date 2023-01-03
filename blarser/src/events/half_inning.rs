use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::entity::{AnyEntity, Game, Team};
use crate::events::{AnyExtrapolated, Effect, Event};
use crate::events::effects::{NullExtrapolated, PitchersExtrapolated};
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct HalfInning {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) top_of_inning: bool,
    pub(crate) inning: i32,
    pub(crate) home_team: Uuid,
    pub(crate) away_team: Uuid,
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
        let mut effects = vec![
            Effect::one_id_with(EntityType::Game, self.game_update.game_id, PitchersExtrapolated::new())
        ];

        if self.top_of_inning && self.inning == 1 {
            effects.push(Effect::one_id(EntityType::Team, self.away_team));
            effects.push(Effect::one_id(EntityType::Team, self.home_team));
        }

        effects
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            let extrapolated: &PitchersExtrapolated = extrapolated.try_into()
                .expect("Wrong extrapolated type in HalfInning update");

            self.game_update.forward(game);

            if game.inning == -1 {
                game.home.pitcher = Some(extrapolated.home.pitcher_id);
                game.home.pitcher_name = Some(extrapolated.home.pitcher_name.clone());
                game.home.pitcher_mod = extrapolated.home.pitcher_mod.clone();
                game.away.pitcher = Some(extrapolated.away.pitcher_id);
                game.away.pitcher_name = Some(extrapolated.away.pitcher_name.clone());
                game.away.pitcher_mod = extrapolated.away.pitcher_mod.clone();
            }

            game.top_of_inning = !game.top_of_inning;
            if game.top_of_inning {
                game.inning += 1;
            }
            game.phase = 6;
            // Just guessing how this works
            game.game_start_phase = if game.inning == 0 { 10 } else { 11 };
            game.half_inning_score = 0.0;
        } else if let Some(team) = entity.as_team_mut() {
            // shrug emoji
            if team.shame_runs > 0. {
                team.shame_runs = 0.;
            }
        }

        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched entity type");
                let extrapolated: &mut PitchersExtrapolated = extrapolated.try_into()
                    .expect("Mismatched extrapolated type");
                extrapolated.home.pitcher_id = new_game.home.pitcher.unwrap();
                extrapolated.home.pitcher_name = new_game.home.pitcher_name.clone().unwrap();
                extrapolated.home.pitcher_mod = new_game.home.pitcher_mod.clone();
                extrapolated.away.pitcher_id = new_game.away.pitcher.unwrap();
                extrapolated.away.pitcher_name = new_game.away.pitcher_name.clone().unwrap();
                extrapolated.away.pitcher_mod = new_game.away.pitcher_mod.clone();

                new_game.half_inning_score = old_game.half_inning_score;
                new_game.game_start_phase = old_game.game_start_phase;
                new_game.phase = old_game.phase;
                if new_game.top_of_inning {
                    new_game.inning -= 1;
                }
                new_game.top_of_inning = !new_game.top_of_inning;

                if new_game.inning == -1 {
                    new_game.home.pitcher = old_game.home.pitcher;
                    new_game.home.pitcher_name = old_game.home.pitcher_name.clone();
                    new_game.home.pitcher_mod = old_game.home.pitcher_mod.clone();
                    new_game.away.pitcher = old_game.away.pitcher;
                    new_game.away.pitcher_name = old_game.away.pitcher_name.clone();
                    new_game.away.pitcher_mod = old_game.away.pitcher_mod.clone();
                }

                self.game_update.reverse(old_game, new_game);
            }
            AnyEntity::Team(old_team) => {
                let new_team: &mut Team = new_parent.try_into()
                    .expect("Mismatched event types");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");
                new_team.shame_runs = old_team.shame_runs;
            }
            _ => {
                panic!("Mismatched extrapolated type")
            }
        }
    }
}

impl Display for HalfInning {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HalfInning for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(HalfInning);