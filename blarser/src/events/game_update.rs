use fed::ScoreInfo;
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
    pub(crate) scores: Option<ScoreInfo>,
    pub(crate) description: String,
}

impl GameUpdate {
    pub fn forward(&self, game: &mut Game) {
        game.play_count = self.play + 1;

        game.last_update = Some(self.description.clone());

        game.last_update_full = None;

        if let Some(score) = &self.scores && !score.scoring_players.is_empty() {
            let mut runs_scored = 0.;
            for score in &score.scoring_players {
                assert_eq!(&score.player_id, game.base_runners.first().unwrap());
                game.pop_base_runner();
                runs_scored += 1.;
            }
            game.score_update = Some(format!("{runs_scored} Run{} scored!",
                                             if runs_scored != 1. { "s" } else { "" }));
            game.half_inning_score += runs_scored;
            *game.team_at_bat_mut().score.as_mut().unwrap() += runs_scored;
            if game.top_of_inning {
                game.top_inning_score += runs_scored;
            } else {
                game.bottom_inning_score += runs_scored;
            }
        } else {
            game.score_update = Some(String::new());
        }

        // TODO Check the conditionals on this
        game.shame = (game.inning > 8 || game.inning > 7 && !game.top_of_inning) &&
            game.home.score.unwrap() > game.away.score.unwrap();
    }

    pub fn reverse(&self, old_game: &Game, new_game: &mut Game) {
        new_game.play_count = self.play;

        new_game.last_update = old_game.last_update.clone();

        new_game.last_update_full = old_game.last_update_full.clone();

        new_game.score_update = old_game.score_update.clone();
        new_game.score_ledger = old_game.score_ledger.clone();

        new_game.score_update = old_game.score_update.clone();
        // TODO Logic about who was where based on how they advanced
        if let Some(score) = &self.scores && !score.scoring_players.is_empty() {
            for _ in &score.scoring_players {
                new_game.base_runners.insert(0, old_game.base_runners.first().unwrap().clone());
                new_game.base_runner_names.insert(0, old_game.base_runner_names.first().unwrap().clone());
                new_game.base_runner_mods.insert(0, old_game.base_runner_mods.first().unwrap().clone());
                new_game.bases_occupied.insert(0, old_game.bases_occupied.first().unwrap().clone());
                new_game.baserunner_count += 1;
            }
            new_game.half_inning_score = old_game.half_inning_score;
            new_game.team_at_bat_mut().score = old_game.team_at_bat().score;
            if new_game.top_of_inning {
                new_game.top_inning_score = old_game.top_inning_score;
            } else {
                new_game.bottom_inning_score = old_game.bottom_inning_score;
            }

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