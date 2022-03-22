use std::iter;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use crate::api::{EventType, EventuallyEvent};
use crate::entity::{Game, UpdateFull};

#[derive(Serialize, Deserialize)]
pub struct Score {
    score_update: String,
    score_ledger: String,
    home_score: f32,
    away_score: f32,
}

#[derive(Serialize, Deserialize)]
pub struct GameUpdate {
    play_count: i64,
    last_update_full: Vec<UpdateFull>,
    score: Option<Score>,
}

impl GameUpdate {
    pub fn parse(event: EventuallyEvent) -> GameUpdate {
        let last_update_full = event.metadata.siblings.iter().map(|event| {
            let team_tags = match event.r#type {
                EventType::AddedMod | EventType::RunsScored | EventType::WinCollectedRegular => {
                    // There's a chance it's always the first id... but I doubt it. Probably have
                    // to check which team the player from playerTags is on
                    vec![*event.team_tags.first()
                        .expect("teamTags must be populated in AddedMod event")]
                }
                EventType::GameEnd => { event.team_tags.clone() }
                _ => Vec::new()
            };

            let metadata = serde_json::from_value(event.metadata.other.clone())
                .expect("Couldn't get metadata from event");
            UpdateFull {
                id: event.id,
                day: event.day,
                nuts: 0, // Always zero theoretically because it hasn't been upnuttable
                r#type: event.r#type as i32,
                blurb: String::new(), // todo ?
                phase: event.phase, // todo ?
                season: event.season,
                created: event.created,
                category: event.category,
                game_tags: Vec::new(),
                team_tags,
                player_tags: event.player_tags.clone(),
                tournament: event.tournament,
                description: event.description.clone(),
                metadata,
            }
        }).collect();

        let score_event = event.metadata.siblings.iter()
            .filter(|event| event.r#type == EventType::RunsScored)
            .at_most_one()
            .expect("Expected at most one RunsScored event");

        let score = if let Some(score_event) = score_event {
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct RunsScoredMetadata {
                ledger: String,
                update: String,
                away_score: f32,
                home_score: f32,
            }

            let runs_metadata: RunsScoredMetadata = serde_json::from_value(score_event.metadata.other.clone())
                .expect("Error parsing RunsScored event metadata");

            Some(Score {
                score_update: runs_metadata.update,
                score_ledger: runs_metadata.ledger,
                home_score: runs_metadata.home_score,
                away_score: runs_metadata.away_score,
            })
        } else {
            None
        };

        Self {
            play_count: event.metadata.play.expect("Game event must have a play count"),
            last_update_full,
            score,
        }
    }
}

impl GameUpdate {
    pub fn forward(&self, game: &mut Game) {
        // play and playCount are out of sync by exactly 1
        game.play_count = 1 + self.play_count;

        // last_update is all the descriptions of the sibling events, separated by \n, and with an
        // extra \n at the end
        game.last_update = self.last_update_full.iter()
            .map(|e| &e.description)
            // This is a too-clever way of getting the extra \n at the end
            .chain(iter::once(&String::new()))
            .join("\n");


        game.last_update_full = Some(self.last_update_full.clone());

        game.score_update = self.score.as_ref().map(|s| s.score_update.clone()).unwrap_or(String::new());
        game.score_ledger = self.score.as_ref().map(|s| s.score_ledger.clone()).unwrap_or(String::new());

        if let Some(score) = &self.score {
            let home_scored = score.home_score - game.home.score
                .expect("homeScore must exist during a game event");
            let away_scored = score.away_score - game.away.score
                .expect("awayScore must exist during a game event");
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
}