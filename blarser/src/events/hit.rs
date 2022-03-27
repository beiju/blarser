use chrono::{DateTime, Utc};
use diesel::QueryResult;
use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::{Finish, IResult, Parser};
use nom::combinator::eof;
use nom::sequence::terminated;
use nom_supreme::error::ErrorTree;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::{AnyEntity, Base, RunnerAdvancement};
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GamePitch;
use crate::events::parse_utils::{collate_siblings, generate_runner_advancements, greedy_text};
use crate::state::StateInterface;

#[derive(Serialize, Deserialize)]
pub struct HitParsed {
    batter_name: String,
    to_base: Base,
}

#[derive(Serialize, Deserialize)]
pub struct Hit {
    game_update: GamePitch,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: HitParsed,
    batter_id: Uuid,
}

pub fn parse_hit(input: &str) -> IResult<&str, HitParsed, ErrorTree<&str>> {
    let (input, batter_name) = greedy_text(tag(" hits a ")).parse(input)?;
    let (input, _) = tag(" hits a ")(input)?;
    let (input, hit_type_str) = alt((tag("Single"), tag("Double"), tag("Triple")))(input)?;
    let (input, _) = terminated(tag("!"), eof)(input)?;

    let to_base = match hit_type_str {
        "Single" => Base::First,
        "Double" => Base::Second,
        "Triple" => Base::Third,
        other => panic!("Unexpected hit type {}", other),
    };

    Ok((input, HitParsed {
        batter_name: batter_name.to_string(),
        to_base,
    }))
}

impl Hit {
    pub fn parse(feed_event: &EventuallyEvent, state: &StateInterface) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("Hit event must have a game id");

        let collated = collate_siblings(&feed_event.metadata.siblings);
        let action_event = collated.action.iter()
            .exactly_one()
            .expect("Expected Hit event to have exactly one action event");

        let event = Self {
            game_update: GamePitch::parse(feed_event),
            time,
            parsed: parse_hit(&action_event.description).finish()
                .expect("Failed to parse Hit from feed event description").1,
            batter_id: feed_event.player_id()
                .expect("Hit event must have exactly one player id"),
        };

        let advance_at_least = (event.parsed.to_base as i32) + 1;

        let possible_advancements = state.read_game_flat(game_id, |game| {
            generate_runner_advancements(&game.base_runners, &game.bases_occupied, advance_at_least)
        })?;

        let effects = possible_advancements.into_iter()
            .map(|advancements| (
                "game".to_string(),
                Some(game_id),
                serde_json::to_value(advancements)
                    .expect("Error serializing possible advancements in Hit event")
            ))
            .collect();

        Ok((AnyEvent::Hit(event), effects))
    }
}

impl Event for Hit {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity {
        let advancements: Vec<RunnerAdvancement> = serde_json::from_value(aux)
            .expect("Error deserializing possible advancements in Hit event");
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                let batter_id = game.team_at_bat().batter
                    .expect("Batter must exist during Hit event");
                let batter_name = game.team_at_bat().batter_name.clone()
                    .expect("Batter name must exist during Hit event");

                assert_eq!(self.batter_id, batter_id,
                           "Batter in Hit event didn't match batter in game state");

                game.advance_runners(&advancements);
                let batter_mod = game.team_at_bat().batter_mod.clone();
                game.push_base_runner(batter_id, batter_name.clone(), batter_mod, self.parsed.to_base);
                game.end_at_bat();


                game.into()
            }
            other => panic!("Hit event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}