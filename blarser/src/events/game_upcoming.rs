use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::entity::{AnyEntity, Game};
use crate::events::{AnyExtrapolated, Effect, Event, ord_by_time};
use crate::events::effects::OddsAndPitchersExtrapolated;
use crate::ingest::StateGraph;
use crate::state::EntityType;

#[derive(Debug, Serialize, Deserialize)]
pub struct GameUpcoming {
    time: DateTime<Utc>,
    game_id: Uuid
}

impl GameUpcoming {
    pub fn new(time: DateTime<Utc>, game_id: Uuid) -> Self {
        GameUpcoming { time, game_id }
    }
}

impl Event for GameUpcoming {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn effects(&self, _: &StateGraph) -> Vec<Effect> {
        vec![
            Effect::one_id_with(EntityType::Game, self.game_id, OddsAndPitchersExtrapolated::default()),
        ]
    }

    fn forward(&self, entity: &AnyEntity, extrapolated: &AnyExtrapolated) -> AnyEntity {
        let mut entity = entity.clone();

         if let Some(game) = entity.as_game_mut() {
            let extrapolated: &OddsAndPitchersExtrapolated = extrapolated.try_into()
                .expect("Mismatched extrapolated type");
            for (self_by_team, odds_extrapolated, pitcher_extrapolated) in [
                (&mut game.home, extrapolated.home_odds, &extrapolated.home),
                (&mut game.away, extrapolated.away_odds, &extrapolated.away)
            ] {
                self_by_team.batter_name = Some(String::new());
                self_by_team.odds = Some(odds_extrapolated);
                self_by_team.pitcher = Some(pitcher_extrapolated.pitcher_id);
                self_by_team.pitcher_name = Some(pitcher_extrapolated.pitcher_name.clone());
                self_by_team.pitcher_mod = pitcher_extrapolated.pitcher_mod.clone();
                self_by_team.score = Some(0.0);
                self_by_team.strikes = Some(3);
            }
            game.last_update = Some(String::new());
            // This starts happening in short circuits, I think
            // game.last_update_full = Some(Vec::new());
        }

        entity
    }

    fn reverse(&self, old_parent: &AnyEntity, extrapolated: &mut AnyExtrapolated, new_parent: &mut AnyEntity) {
        match old_parent {
            AnyEntity::Game(old_game) => {
                let new_game: &mut Game = new_parent.try_into()
                    .expect("Mismatched event types");
                let extrapolated: &mut OddsAndPitchersExtrapolated = extrapolated.try_into()
                    .expect("Extrapolated type mismatch");
                extrapolated.away.pitcher_id = new_game.away.pitcher
                    .expect("Away pitcher should exist when reversing an GameUpcoming event");
                extrapolated.away.pitcher_name = new_game.away.pitcher_name.clone()
                    .expect("Away pitcher should exist when reversing an GameUpcoming event");
                extrapolated.away.pitcher_mod = new_game.away.pitcher_mod.clone();
                extrapolated.home.pitcher_id = new_game.home.pitcher
                    .expect("Home pitcher should exist when reversing an GameUpcoming event");
                extrapolated.home.pitcher_name = new_game.home.pitcher_name.clone()
                    .expect("Home pitcher should exist when reversing an GameUpcoming event");
                extrapolated.home.pitcher_mod = new_game.home.pitcher_mod.clone();
                extrapolated.away_odds = new_game.away.odds
                    .expect("Odds should exist when reversing an GameUpcoming event");
                extrapolated.home_odds = new_game.home.odds
                    .expect("Odds should exist when reversing an GameUpcoming event");

                for (old_by_team, new_by_team) in [
                    (&old_game.home, &mut new_game.home),
                    (&old_game.away, &mut new_game.away),
                ] {
                    new_by_team.batter_name = old_by_team.batter_name.clone();
                    new_by_team.odds = old_by_team.odds;
                    new_by_team.pitcher = old_by_team.pitcher;
                    new_by_team.pitcher_name = old_by_team.pitcher_name.clone();
                    new_by_team.pitcher_mod = old_by_team.pitcher_mod.clone();
                    new_by_team.score = old_by_team.score;
                    new_by_team.strikes = old_by_team.strikes;
                }
                new_game.last_update = old_game.last_update.clone();
                new_game.last_update_full = old_game.last_update_full.clone();
            }
            _ => {
                panic!("Can't reverse-apply GameUpcoming to this entity type");
            }
        }
    }
}

impl Display for GameUpcoming {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GameUpcoming at {}", self.time)
    }
}

ord_by_time!(GameUpcoming);