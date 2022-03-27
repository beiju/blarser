use chrono::{DateTime, Utc};
use diesel::QueryResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::digit1;
use nom::{Finish, IResult, Parser};
use nom::combinator::eof;
use nom::sequence::terminated;
use nom_supreme::error::ErrorTree;
use nom_supreme::ParserExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::{GamePitch, GameUpdate};
use crate::events::parse_utils::greedy_text;

#[derive(Serialize, Deserialize)]
pub struct Count {
    pub balls: i32,
    pub strikes: i32,
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
    game_update: GamePitch,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: StrikeParsed,
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
            game_update: GamePitch::parse(feed_event),
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

#[derive(Serialize, Deserialize)]
pub struct Ball {
    game_update: GamePitch,
    time: DateTime<Utc>,
    count: Count,
}

pub fn parse_ball(input: &str) -> IResult<&str, Count, ErrorTree<&str>> {
    let (input, _) = tag("Ball. ")(input)?;
    parse_count.all_consuming().parse(input)
}

impl Ball {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("Ball event must have a game id");

        let event = Self {
            game_update: GamePitch::parse(feed_event),
            time,
            count: parse_ball(&feed_event.description).finish()
                .expect("Failed to parse Ball from feed event description").1
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::Ball(event), effects))
    }
}

impl Event for Ball {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.at_bat_balls += 1;

                game.into()
            }
            other => panic!("Ball event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
pub struct FoulBall {
    game_update: GamePitch,
    time: DateTime<Utc>,
    count: Count,
}

// One day this will probably have Very Foul Ball parsing in it but not this day
pub fn parse_foul_ball(input: &str) -> IResult<&str, Count, ErrorTree<&str>> {
    let (input, _) = tag("Foul Ball. ")(input)?;
    parse_count.all_consuming().parse(input)
}

impl FoulBall {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("FoulBall event must have a game id");

        let event = Self {
            game_update: GamePitch::parse(feed_event),
            time,
            count: parse_foul_ball(&feed_event.description).finish()
                .expect("Failed to parse FoulBall from feed event description").1
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::FoulBall(event), effects))
    }
}

impl Event for FoulBall {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                let strikes_to_strike_out = game.team_at_bat().strikes
                    .expect("{home/away}Strikes must be set during FoulBall event");
                if game.at_bat_strikes + 1 < strikes_to_strike_out {
                    game.at_bat_strikes += 1;
                }

                game.into()
            }
            other => panic!("FoulBall event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
pub enum StrikeoutType {
    Swinging,
    Looking,
}

#[derive(Serialize, Deserialize)]
pub struct StrikeoutParsed {
    batter_name: String,
    strikeout_type: StrikeoutType,
}

#[derive(Serialize, Deserialize)]
pub struct Strikeout {
    game_update: GamePitch,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: StrikeoutParsed,
}

pub fn parse_strikeout(input: &str) -> IResult<&str, StrikeoutParsed, ErrorTree<&str>> {
    let (input, batter_name) = greedy_text(tag(" strikes out ")).parse(input)?;
    let (input, _) = tag(" strikes out ")(input)?;
    let (input, strikeout_str) = alt((tag("swinging"), tag("looking")))(input)?;
    let (input, _) = terminated(tag("."), eof)(input)?;

    let strikeout_type = match strikeout_str {
        "swinging" => StrikeoutType::Swinging,
        "looking" => StrikeoutType::Looking,
        other => panic!("Unexpected strikeout type {}", other),
    };

    Ok((input, StrikeoutParsed {
        batter_name: batter_name.to_string(),
        strikeout_type
    }))
}

impl Strikeout {
    pub fn parse(feed_event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("Strikeout event must have a game id");

        let event = Self {
            game_update: GamePitch::parse(feed_event),
            time,
            parsed: parse_strikeout(&feed_event.description).finish()
                .expect("Failed to parse Strikeout from feed event description").1
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::Strikeout(event), effects))
    }
}

impl Event for Strikeout {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, _: serde_json::Value) -> AnyEntity {
        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.out(1);

                game.into()
            }
            other => panic!("Strikeout event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}