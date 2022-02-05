use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::event_utils;
use crate::sim::{Entity, FeedEventChangeResult, Game};
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Standings {
    pub id: Uuid,
    pub runs: HashMap<Uuid, f32>,
    pub wins: HashMap<Uuid, i32>,
    pub losses: HashMap<Uuid, i32>,
    pub games_played: HashMap<Uuid, i32>,
}

impl Entity for Standings {
    fn name() -> &'static str {
        "standings"
    }

    fn next_timed_event(&self, _from_time: DateTime<Utc>, _to_time: DateTime<Utc>, _state: &StateInterface) -> Option<GenericEvent> {
        None
    }

    fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult {
        match &event.event_type {
            GenericEventType::FeedEvent(event) => self.apply_feed_event(event, state),
            other => {
                panic!("{:?} event does not apply to standings", other)
            }
        }
    }
}

impl Standings {
    fn apply_feed_event(&mut self, event: &EventuallyEvent, state: &StateInterface) -> FeedEventChangeResult {
        match event.r#type {
            EventType::GameOver => {
                let game_id = event_utils::get_one_id(&event.game_tags, "gameTags");
                let game: Game = state.entity(*game_id, event.created);

                let (winner_id, loser_id) = if game.away.score.unwrap() > game.home.score.unwrap() {
                    (game.away.team, game.home.team)
                } else {
                    (game.home.team, game.away.team)
                };

                *self.games_played.entry(winner_id).or_insert(0) += 1;
                *self.games_played.entry(loser_id).or_insert(0) += 1;
                *self.wins.entry(winner_id).or_insert(0) += 1;
                *self.losses.entry(loser_id).or_insert(0) += 1;
                *self.runs.entry(game.away.team).or_insert(0.0) += game.away.score
                    .expect("Away team must have a score during GameOver event");
                *self.runs.entry(game.home.team).or_insert(0.0) += game.home.score
                    .expect("Home team must have a score during GameOver event");

                FeedEventChangeResult::Ok
            }
            other => {
                panic!("{:?} event does not apply to standings", other)
            }
        }
    }
}