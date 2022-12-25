use fed::{FreeRefill, ScoreInfo, ScoringPlayer};
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
            Self::score(game, &score.scoring_players, &score.free_refills);
        } else {
            game.score_update = Some(String::new());
        }

        // TODO Check the conditionals on this
        game.shame = (game.inning > 8 || game.inning > 7 && !game.top_of_inning) &&
            game.home.score.unwrap() > game.away.score.unwrap();
    }

    pub fn score(game: &mut Game, scoring_players: &[ScoringPlayer], free_refills: &[FreeRefill]) {
        let mut runs_scored = 0.;
        for score in scoring_players {
            game.pop_base_runner(score.player_id);
            runs_scored += 1.;
        }
        game.score_update = Some(format!("{runs_scored} Run{} scored!",
                                         if runs_scored != 1. { "s" } else { "" }));
        game.half_inning_score += runs_scored;
        *game.team_at_bat_mut().score.as_mut().unwrap() += runs_scored;
        *game.current_half_score_mut() += runs_scored;
        // There cant be free refills without scores [falsehoods] so it's fine to do this here
        game.half_inning_outs -= free_refills.len() as i32;
    }

    pub fn reverse(&self, old_game: &Game, new_game: &mut Game) {
        new_game.play_count = self.play;

        new_game.last_update = old_game.last_update.clone();

        new_game.last_update_full = old_game.last_update_full.clone();

        new_game.score_update = old_game.score_update.clone();
        new_game.score_ledger = old_game.score_ledger.clone();

        new_game.score_update = old_game.score_update.clone();
        if let Some(score) = &self.scores && !score.scoring_players.is_empty() {
            Self::reverse_score(old_game, new_game, &score.scoring_players, &score.free_refills);
        }

        new_game.shame = old_game.shame;
    }

    pub fn reverse_score(old_game: &Game, new_game: &mut Game, scoring_players: &[ScoringPlayer], free_refills: &[FreeRefill]) {
        // I think re-using the iterator will let us properly handle multiple of the same
        // player. Using enumerate to get index rather than find_position because I think
        // find_position will reset the index.
        //
        // This is made much more complicated by just a few games where players could score
        // from positions other than the front of the array.
        let mut old_base_runners_it = old_game.base_runners.iter()
            .enumerate();
        for scorer in scoring_players {
            let (idx, _) = old_base_runners_it
                .find(|(_, &id)| id == scorer.player_id)
                .expect("The scorer must be present in the base_runners list");
            new_game.base_runners.insert(idx, old_game.base_runners[idx].clone());
            new_game.base_runner_names.insert(idx, old_game.base_runner_names[idx].clone());
            new_game.base_runner_mods.insert(idx, old_game.base_runner_mods[idx].clone());
            new_game.bases_occupied.insert(idx, old_game.bases_occupied[idx].clone());
            new_game.baserunner_count += 1;
        }
        new_game.half_inning_score = old_game.half_inning_score;
        new_game.team_at_bat_mut().score = old_game.team_at_bat().score;
        *new_game.current_half_score_mut() = old_game.current_half_score();
        // There cant be free refills without scores [falsehoods] so it's fine to do this here
        new_game.half_inning_outs += free_refills.len() as i32;
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