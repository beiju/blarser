use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;

use crate::entity::{AnyEntity, Entity, EntityRaw};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, PartialInformationCompare)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Standings {
    #[serde(rename = "__v")]
    pub version: Option<i32>,

    #[serde(alias = "_id")]
    pub id: Uuid,

    #[serde(default)]
    pub runs: HashMap<Uuid, f32>,
    pub wins: HashMap<Uuid, i32>,
    pub losses: HashMap<Uuid, i32>,
    #[serde(default)]
    pub games_played: HashMap<Uuid, i32>,
}

impl Display for Standings {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Standings")
    }
}

impl EntityRaw for <Standings as PartialInformationCompare>::Raw {
    type Entity = Standings;

    fn name() -> &'static str { "standings" }
    fn id(&self) -> Uuid { self.id }

    // It's definitely timestamped after when it's extracted from streamData, but it may also be
    // polled and timestamped before in that case
    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from - Duration::minutes(1)
    }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from + Duration::minutes(1)
    }
}

impl Into<AnyEntity> for Standings {
    fn into(self) -> AnyEntity {
        AnyEntity::Standings(self)
    }
}

impl Entity for Standings {
    fn name() -> &'static str { "standings" }
    fn id(&self) -> Uuid { self.id }
}
// impl Standings {
//     fn apply_feed_event(&mut self, event: &EventuallyEvent, state: &StateInterface) -> FeedEventChangeResult {
//         match event.r#type {
//             EventType::GameEnd => {
//                 let winner_id: Uuid = serde_json::from_value(
//                     event.metadata.other.get("winner")
//                         .expect("GameEnd event must have a winner in the metadata")
//                         .clone())
//                     .expect("Winner property of GameEnd event must be a uuid");
//
//                 let loser_id = *event.team_tags.iter()
//                     .filter(|&id| *id != winner_id)
//                     .exactly_one()
//                     .expect("gameTags of GameEnd event must contain exactly one winner and one loser");
//
//                 let deadline = event.created + Duration::minutes(5);
//                 self.games_played.add_with_default(winner_id, 1, deadline);
//                 self.games_played.add_with_default(loser_id, 1, deadline);
//                 self.wins.add_with_default(winner_id, 1, deadline);
//                 self.wins.add_with_default(loser_id, 0, deadline);
//                 self.losses.add_with_default(winner_id, 0, deadline);
//                 self.losses.add_with_default(loser_id, 1, deadline);
//
//                 let game: Game = state.entity(
//                     *event.game_tags.iter().exactly_one()
//                         .expect("GameEnd event must have exactly one game tag"),
//                     event.created);
//
//                 self.runs.add_with_default(game.away.team,
//                                            game.away.score.expect("Away team must have a score during GameOver event"),
//                                            deadline);
//                 self.runs.add_with_default(game.home.team,
//                                            game.home.score.expect("Home team must have a score during GameOver event"),
//                                            deadline);
//
//                 FeedEventChangeResult::Ok
//             }
//             other => {
//                 panic!("{:?} event does not apply to standings", other)
//             }
//         }
//     }
// }