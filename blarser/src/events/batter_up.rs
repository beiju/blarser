use chrono::{DateTime, Utc};
use diesel::QueryResult;
use nom::{bytes::complete::tag, Finish, IResult, Parser};
use nom_supreme::{error::ErrorTree, ParserExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;
use crate::events::nom_utils::greedy_text;

#[derive(Clone, Serialize, Deserialize)]
pub struct WhichBatter {
    pub batter_name: String,
    pub batter_team_nickname: String,
}

#[derive(Serialize, Deserialize)]
pub struct BatterUp {
    game_update: GameUpdate,
    time: DateTime<Utc>,
    #[serde(flatten)]
    which_batter: WhichBatter,
    batter_id: Uuid,
}

pub fn parse_which_batter(input: &str) -> IResult<&str, WhichBatter, ErrorTree<&str>> {
    let (input, batter_name) = greedy_text(tag(" batting for the ")).parse(input)?;
    let (input, _) = tag(" batting for the ")(input)?;
    let (input, batter_team_nickname) = greedy_text(tag(".").all_consuming()).parse(input)?;
    let (input, _) = tag(".").all_consuming().parse(input)?;

    Ok((input, WhichBatter {
        batter_name: batter_name.to_string(),
        batter_team_nickname: batter_team_nickname.to_string()
    }))
}

impl BatterUp {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("BatterUp event must have a game id");
        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time,
            which_batter: parse_which_batter(&feed_event.description).finish()
                .expect("Error parsing text of BatterUp event").1,
            batter_id: feed_event.player_id().expect("BatterUp event must have exactly one player id"),
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::BatterUp(event), effects))
    }
}

impl Event for BatterUp {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                let prev_batter_count = game.team_at_bat().team_batter_count
                    .expect("TeamBatterCount must be populated during a game");
                game.team_at_bat_mut().team_batter_count = Some(prev_batter_count + 1);
                game.team_at_bat_mut().batter = Some(self.batter_id);
                game.team_at_bat_mut().batter_name = Some(self.which_batter.batter_name.clone());

                game.into()
            }
            other => panic!("BatterUp event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}