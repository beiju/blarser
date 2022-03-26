use chrono::{DateTime, Utc};
use diesel::QueryResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::digit1;
use nom::{Finish, IResult, Parser};
use nom_supreme::error::ErrorTree;
use nom_supreme::ParserExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;

#[derive(Serialize, Deserialize)]
pub struct Count {
    pub balls: i32,
    pub strikes: i32,
}

#[derive(Serialize, Deserialize)]
pub enum StrikeType {
    Swinging,
    Looking,
    Flinching,
}

#[derive(Serialize, Deserialize)]
pub struct StrikeParsed {
    pub strike_type: StrikeType,
    pub count: Count,
}

#[derive(Serialize, Deserialize)]
pub struct Strike {
    game_update: GameUpdate,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: StrikeParsed,
}

pub fn parse_count(input: &str) -> IResult<&str, Count, ErrorTree<&str>> {
    let (input, balls_str) = digit1(input)?;
    let (input, _) = tag("-")(input)?;
    let (input, strikes_str) = digit1(input)?;

    Ok((input, Count {
        balls: balls_str.parse().expect("Failed to convert balls in count to integer"),
        strikes: strikes_str.parse().expect("Failed to convert strikes in count to integer"),
    }))
}

pub fn parse_strike(input: &str) -> IResult<&str, StrikeParsed, ErrorTree<&str>> {
    let (input, _) = tag("Strike, ")(input)?;
    let (input, strike_str) = alt((tag("swinging"), tag("looking"), tag("flinching")))(input)?;
    let (input, _) = tag(". ")(input)?;
    let (input, count) = parse_count.all_consuming().parse(input)?;

    let strike_type = match strike_str {
        "swinging" => StrikeType::Swinging,
        "looking" => StrikeType::Looking,
        "flinching" => StrikeType::Flinching,
        other => panic!("Unexpected strike string {}", other),
    };

    Ok((input, StrikeParsed {
        strike_type,
        count
    }))
}

impl Strike {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("Strike event must have a game id");

        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time,
            parsed: parse_strike(&feed_event.description).finish()
                .expect("Failed to parse Strike from feed event description").1
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::Strike(event), effects))
    }
}

impl Event for Strike {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.at_bat_strikes += 1;

                game.into()
            }
            other => panic!("Strike event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}