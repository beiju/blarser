use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::entity::Game;

#[derive(Debug, Serialize, Deserialize)]
pub struct Score {
    score_update: String,
    score_ledger: String,
    home_score: f32,
    away_score: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GameUpdate {
    pub(crate) game_id: Uuid,
    pub(crate) play: i64,
    // pub(crate) last_update_full: Vec<UpdateFull>,
    pub(crate) score: Option<Score>,
    pub(crate) description: String,
}

impl GameUpdate {
    pub fn forward(&self, game: &mut Game) {
        game.play_count = self.play + 1;

        game.last_update = Some(self.description.clone());

        game.last_update_full = None;

        game.score_update = Some(self.score.as_ref().map(|s| s.score_update.clone()).unwrap_or_default());
        game.score_ledger = Some(self.score.as_ref().map(|s| s.score_ledger.clone()).unwrap_or_default());

        if let Some(score) = &self.score {
            let (home_scored, away_scored) = Self::get_scored(game, score);
            game.half_inning_score += home_scored + away_scored;
            if game.top_of_inning {
                game.top_inning_score += home_scored + away_scored;
            } else {
                game.bottom_inning_score += home_scored + away_scored;
            }
            game.home.score = Some(score.home_score);
            game.away.score = Some(score.away_score);
        }

        // TODO Check the conditionals on this
        game.shame = (game.inning > 8 || game.inning > 7 && !game.top_of_inning) &&
            game.home.score.unwrap() > game.away.score.unwrap();
    }

    fn get_scored(game: &mut Game, score: &Score) -> (f32, f32) {
        let home_scored = score.home_score - game.home.score
            .expect("homeScore must exist during a game event");
        let away_scored = score.away_score - game.away.score
            .expect("awayScore must exist during a game event");
        (home_scored, away_scored)
    }

    pub fn reverse(&self, old_game: &Game, new_game: &mut Game) {
        new_game.play_count = self.play;

        new_game.last_update = old_game.last_update.clone();

        new_game.last_update_full = old_game.last_update_full.clone();

        new_game.score_update = old_game.score_update.clone();
        new_game.score_ledger = old_game.score_ledger.clone();

        if let Some(score) = &self.score {
            let (home_scored, away_scored) = Self::get_scored(new_game, score);
            new_game.half_inning_score -= home_scored + away_scored;
            if new_game.top_of_inning {
                new_game.top_inning_score -= home_scored + away_scored;
            } else {
                new_game.bottom_inning_score -= home_scored + away_scored;
            }
            new_game.home.score = old_game.home.score;
            new_game.away.score = old_game.away.score;
        }

        new_game.shame = old_game.shame;
    }
}

// #[derive(Serialize, Deserialize)]
// #[serde(transparent)]
// pub struct GamePitch(GameUpdate);
//
// impl GamePitch {
//     pub fn forward(&self, game: &mut Game) {
//         self.0.forward(game);
//
//         // if game.weather == (Weather::Snowy as i32) && game.state.map_or(false, |state| state.snowfall_events.is_none()) {
//         //     game.state.snowfall_events = Some(0);
//         // }
//     }
// }