use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::{Conflict, PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;

use crate::entity::{Entity, EntityRaw, EntityRawTrait, EntityTrait};

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

impl EntityRawTrait for <Standings as PartialInformationCompare>::Raw {
    fn entity_type(&self) -> &'static str { "standings" }
    fn entity_id(&self) -> Uuid { self.id }

    // It's definitely timestamped after when it's extracted from streamData, but it may also be
    // polled and timestamped before in that case
    fn earliest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from - Duration::minutes(1)
    }

    fn latest_time(&self, valid_from: DateTime<Utc>) -> DateTime<Utc> {
        valid_from + Duration::minutes(1)
    }

    fn as_entity(self) -> Entity {
        Entity::Standings(Standings::from_raw(self))
    }
    fn to_json(self) -> serde_json::Value {
        serde_json::to_value(self)
            .expect("Error serializing StandingsRaw object")
    }
}

impl EntityTrait for Standings {
    fn entity_type(&self) -> &'static str { "standings" }
    fn entity_id(&self) -> Uuid { self.id }

    fn observe(&mut self, raw: &EntityRaw) -> Vec<Conflict> {
        if let EntityRaw::Standings(raw) = raw {
            PartialInformationCompare::observe(self, raw)
        } else {
            panic!("Tried to observe {} with an observation from {}",
                   self.entity_type(), raw.entity_type());
        }
    }
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