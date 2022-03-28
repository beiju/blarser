use chrono::{DateTime, Utc};
use diesel::QueryResult;
use itertools::Itertools;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::{Finish, IResult, Parser};
use nom::character::complete::digit1;
use nom::combinator::eof;
use nom::sequence::terminated;
use nom_supreme::error::ErrorTree;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::{EventType, EventuallyEvent};
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;
use crate::events::parse_utils::greedy_text;

#[derive(Serialize, Deserialize)]
pub struct PlayerNameId {
    pub player_name: String,
    pub player_id: Uuid,
}
#[derive(Serialize, Deserialize)]
pub enum SnowfallType {
    Slightly,
    Normal,
    Greatly,
}

#[derive(Serialize, Deserialize)]
pub struct SnowParsed {
    pub snowfall_players: Vec<PlayerNameId>,
    pub num_snowflakes: i32,
    pub snowfall_type: SnowfallType,
    pub frozen_players: Vec<PlayerNameId>,
}

pub fn parse(event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
    let events = event.metadata.siblings.as_slice();

    let (split_i, first_non_reroll_event) = events.iter()
        .find_position(|event| event.r#type != EventType::PlayerStatReroll)
        .expect("PlayerStatReroll events must have at least one non-PlayerStatReroll sibling");

    assert_eq!(first_non_reroll_event.r#type, EventType::Snowflakes,
               "Unexpected event type {:?} after PlayerStatReroll", first_non_reroll_event.r#type);

    let (rerolls, events) = events.split_at(split_i);
    let (snowflakes, freezings) = events.split_first().unwrap();
    assert!(freezings.iter().all(|event| event.r#type == EventType::AddedMod),
            "Unexpected event type after Snowflakes");

    let snowfall_players = rerolls.iter()
        .map(|event| {
            let (_, player_name) = parse_snowfall(&event.description).finish()
                .expect("Error parsing snowfall event");
            let player_id = event.player_id()
                .expect("PlayerStatReroll event must have exactly one player id");

            PlayerNameId { player_name: player_name.to_string(), player_id }
        })
        .collect();

    let (_, (num_snowflakes, snowfall_type)) = parse_snowflakes(&snowflakes.description).finish()
        .expect("Error parsing snowflakes event");

    let frozen_players = freezings.iter()
        .map(|event| {
            let (_, player_name) = parse_frozen(&event.description).finish()
                .expect("Error parsing freezing event");
            let player_id = event.player_id()
                .expect("AddedMod event must have exactly one player id");

            PlayerNameId { player_name: player_name.to_string(), player_id }
        })
        .collect();

    // TODO Only do this for gamma9
    let mut displayed_feed_event = snowflakes.clone();
    displayed_feed_event.metadata.siblings.push(snowflakes.clone());
    displayed_feed_event.metadata.siblings.extend_from_slice(freezings);

    Snow::from_parsed(event, displayed_feed_event,SnowParsed {
        snowfall_players,
        num_snowflakes,
        snowfall_type,
        frozen_players,
    })
}

fn parse_snowfall(input: &str) -> IResult<&str, &str, ErrorTree<&str>> {
    let (input, _) = tag("Snow fell on ")(input)?;
    let (input, player_name) = greedy_text(terminated(tag("!"), eof)).parse(input)?;
    let (input, _) = terminated(tag("!"), eof)(input)?;


    IResult::Ok((input, player_name))
}

fn parse_snowflakes(input: &str) -> IResult<&str, (i32, SnowfallType), ErrorTree<&str>> {
    let (input, snowflake_amount) = digit1(input)?;
    let (input, _) = tag(" Snowflakes ")(input)?;
    let (input, modified_type) = alt((tag("slightly modified"), tag("modified"), tag("greatly modified")))(input)?;
    let (input, _) = terminated(tag(" the field!"), eof)(input)?;

    let snowflake_amount = snowflake_amount.parse()
        .expect("Error parsing integer for snowflake amount");

    let snowfall_type = match modified_type {
        "slightly modified" => SnowfallType::Slightly,
        "modified" => SnowfallType::Normal,
        "greatly modified" => SnowfallType::Greatly,
        other => panic!("Unexpected value for snowfall modified type: {}", other),
    };

    IResult::Ok((input, (snowflake_amount, snowfall_type)))
}

fn parse_frozen(input: &str) -> IResult<&str, &str, ErrorTree<&str>> {
    let (input, player_name) = greedy_text(terminated(tag(" was Frozen!"), eof)).parse(input)?;
    let (input, _) = terminated(tag(" was Frozen!"), eof)(input)?;


    IResult::Ok((input, player_name))
}

#[derive(Serialize, Deserialize)]
pub struct Snow {
    game_update: GameUpdate,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: SnowParsed,
}

impl Snow {
    pub fn from_parsed(feed_event: &EventuallyEvent, displayed_feed_event: EventuallyEvent, parsed: SnowParsed) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = displayed_feed_event.game_id().expect("Snow event must have a game id");

        let event = Self {
            game_update: GameUpdate::parse(&displayed_feed_event),
            time,
            parsed,
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::Snow(event), effects))
    }
}

impl Event for Snow {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.state.snowfall_events = Some(game.state.snowfall_events.unwrap_or(0) + 1);

                game.into()
            }
            other => panic!("Snow event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}