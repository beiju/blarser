use std::fmt::{Display, Formatter};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;
use crate::entity::Game;

use crate::events::{AnyEffect, Effect, EffectVariant, Event};
use crate::events::effects::{OddsAndPitchersExtrapolated, PitcherExtrapolated};
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

    fn into_effects(self, _: &StateGraph) -> Vec<AnyEffect> {
        vec![GameUpcomingEffect::new(self.game_id).into()]
    }
}

impl Display for GameUpcoming {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GameUpcoming at {}", self.time)
    }
}


#[derive(Debug, Clone)]
pub struct GameUpcomingEffect {
    game_id: Uuid,
}

impl GameUpcomingEffect {
    pub fn new(game_id: Uuid) -> Self { Self { game_id } }
}

impl Effect for GameUpcomingEffect {
    type Variant = GameUpcomingEffectVariant;

    fn entity_type(&self) -> EntityType { EntityType::Game }

    fn entity_id(&self) -> Option<Uuid> { Some(self.game_id) }

    fn variant(&self) -> Self::Variant {
        Self::Variant::default()
    }
}

#[derive(Debug, Default, Clone)]
pub struct GameUpcomingEffectVariant {
    pub away: PitcherExtrapolated,
    pub home: PitcherExtrapolated,
    pub away_odds: MaybeKnown<f32>,
    pub home_odds: MaybeKnown<f32>,
}

impl EffectVariant for GameUpcomingEffectVariant {
    type EntityType = Game;

    fn forward(&self, game: &mut Game) {
        for (self_by_team, odds_extrapolated, pitcher_extrapolated) in [
            (&mut game.home, self.home_odds, &self.home),
            (&mut game.away, self.away_odds, &self.away)
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

    fn reverse(&mut self, old_game: &Game, new_game: &mut Game) {
        self.away.pitcher_id = new_game.away.pitcher
            .expect("Away pitcher should exist when reversing an GameUpcoming event");
        self.away.pitcher_name = new_game.away.pitcher_name.clone()
            .expect("Away pitcher should exist when reversing an GameUpcoming event");
        self.away.pitcher_mod = new_game.away.pitcher_mod.clone();
        self.home.pitcher_id = new_game.home.pitcher
            .expect("Home pitcher should exist when reversing an GameUpcoming event");
        self.home.pitcher_name = new_game.home.pitcher_name.clone()
            .expect("Home pitcher should exist when reversing an GameUpcoming event");
        self.home.pitcher_mod = new_game.home.pitcher_mod.clone();
        self.away_odds = new_game.away.odds
            .expect("Odds should exist when reversing an GameUpcoming event");
        self.home_odds = new_game.home.odds
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
}