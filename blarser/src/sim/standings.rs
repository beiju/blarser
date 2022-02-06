use std::collections::HashMap;
use std::fmt::Debug;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use serde::Deserialize;
use uuid::Uuid;
use partial_information::{CachedMap, PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::api::{EventType, EventuallyEvent};
use crate::sim::{Entity, FeedEventChangeResult, Game};
use crate::state::{StateInterface, GenericEvent, GenericEventType};

#[derive(Clone, Debug, Deserialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Standings {
    pub id: Uuid,
    pub runs: CachedMap<Uuid, f32>,
    pub wins: CachedMap<Uuid, i32>,
    pub losses: CachedMap<Uuid, i32>,
    pub games_played: CachedMap<Uuid, i32>,
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
            EventType::GameEnd => {
                let winner_id: Uuid = serde_json::from_value(
                    event.metadata.other.get("winner")
                        .expect("GameEnd event must have a winner in the metadata")
                        .clone())
                    .expect("Winner property of GameEnd event must be a uuid");

                let loser_id = *event.team_tags.iter()
                    .filter(|&id| *id != winner_id)
                    .exactly_one()
                    .expect("gameTags of GameEnd event must contain exactly one winner and one loser");

                let deadline = event.created + Duration::minutes(5);
                self.games_played.add_with_default(winner_id, 1, deadline);
                self.games_played.add_with_default(loser_id, 1, deadline);
                self.wins.add_with_default(winner_id, 1, deadline);
                self.wins.add_with_default(loser_id, 0, deadline);
                self.losses.add_with_default(winner_id, 0, deadline);
                self.losses.add_with_default(loser_id, 1, deadline);

                let game: Game = state.entity(
                    *event.game_tags.iter().exactly_one()
                        .expect("GameEnd event must have exactly one game tag"),
                    event.created);

                self.runs.add_with_default(game.away.team,
                                           game.away.score.expect("Away team must have a score during GameOver event"),
                                           deadline);
                self.runs.add_with_default(game.home.team,
                                           game.home.score.expect("Home team must have a score during GameOver event"),
                                           deadline);

                FeedEventChangeResult::Ok
            }
            other => {
                panic!("{:?} event does not apply to standings", other)
            }
        }
    }
}