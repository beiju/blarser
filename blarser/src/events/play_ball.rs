use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::entity::{AnyEntity, Game, Team};
use crate::events::{Effect, Event, ord_by_time, AnyExtrapolated};
use crate::events::effects::NullExtrapolated;
use crate::events::game_update::GameUpdate;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct PlayBall {
    pub(crate) game_update: GameUpdate,
    pub(crate) time: DateTime<Utc>,
    pub(crate) home_team: Uuid,
    pub(crate) away_team: Uuid,
}

impl Event for PlayBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id(EntityType::Game, self.game_update.game_id),
            Effect::one_id(EntityType::Team, self.away_team),
            Effect::one_id(EntityType::Team, self.home_team),
        ]
    }

    fn forward(&self, entity: &AnyEntity, _: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();
        if let Some(game) = entity.as_game_mut() {
            self.game_update.forward(game);

            game.game_start_phase = -1; // not sure about this
            game.inning = -1;
            game.phase = 2;
            game.top_of_inning = false;

            // It unsets pitchers :(
            game.home.pitcher = None;
            game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
            game.home.pitcher_mod = MaybeKnown::Known(String::new());
            game.away.pitcher = None;
            game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));
            game.away.pitcher_mod = MaybeKnown::Known(String::new());
        } else if let Some(team) = entity.as_team_mut() {
            team.rotation_slot += 1;
        }

        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched event types");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");

                new_game.home.pitcher = old_game.home.pitcher;
                new_game.home.pitcher_name = old_game.home.pitcher_name.clone();
                new_game.home.pitcher_mod = old_game.home.pitcher_mod.clone();
                new_game.away.pitcher = old_game.away.pitcher;
                new_game.away.pitcher_name = old_game.away.pitcher_name.clone();
                new_game.away.pitcher_mod = old_game.away.pitcher_mod.clone();

                new_game.game_start_phase = old_game.game_start_phase;
                new_game.inning = old_game.inning;
                new_game.phase = old_game.phase;
                new_game.top_of_inning = old_game.top_of_inning;

                self.game_update.reverse(old_game, new_game);
            }
            AnyEntity::Team(old_team) => {
                let new_team: &mut Team = new_parent.try_into()
                    .expect("Mismatched event types");
                let _: &mut NullExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");
                new_team.rotation_slot = old_team.rotation_slot;
            }
            _ => {
                panic!("Can't reverse-apply PlayBall to this entity type");
            }
        }
    }
}

impl Display for PlayBall {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlayBall for {} at {}", self.game_update.game_id, self.time)
    }
}

ord_by_time!(PlayBall);