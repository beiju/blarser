use chrono::{DateTime, Utc};
use diesel::QueryResult;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::{Finish, IResult, Parser};
use nom::combinator::eof;
use nom::sequence::terminated;
use nom_supreme::error::ErrorTree;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::EventuallyEvent;
use crate::entity::{AnyEntity, Base};
use crate::events::{AnyEvent, Event};
use crate::events::game_update::{GamePitch, GameUpdate};
use crate::events::parse_utils::{collate_siblings, greedy_text, parse_base};


#[derive(Serialize, Deserialize)]
pub struct GroundOrFlyOutParsed {
    batter_name: String,
    fielder_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct DoublePlayParsed {
    batter_name: String,
}

#[derive(Serialize, Deserialize)]
pub struct FieldersChoiceParsed {
    batter_name: String,
    runner_out_name: String,
    runner_out_base: Base,
}

enum FieldingOutParsed {
    GroundOut(GroundOrFlyOutParsed),
    Flyout(GroundOrFlyOutParsed),
    DoublePlay(DoublePlayParsed),
    FieldersChoice(FieldersChoiceParsed),
}

pub fn parse(event: &EventuallyEvent) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
    let collated = collate_siblings(&event.metadata.siblings);

    let (_, parse_result) = match collated.action.len() {
        1 => parse_simple_out(&collated.action[0].description),
        2 => parse_fielders_choice(&collated.action[0].description, &collated.action[1].description),
        more => panic!("Unexpected fielding out with {} non-score siblings", more)
    }.finish()
        .expect("Failed to parse fielding out");

    match parse_result {
        FieldingOutParsed::GroundOut(parsed) => {
            GroundOut::from_parsed(event, parsed)
        }
        FieldingOutParsed::Flyout(parsed) => {
            Flyout::from_parsed(event, parsed)
        }
        FieldingOutParsed::DoublePlay(_) => { todo!() }
        FieldingOutParsed::FieldersChoice(_) => { todo!() }
    }
}

fn parse_simple_out(input: &str) -> IResult<&str, FieldingOutParsed, ErrorTree<&str>> {
    alt((parse_single_batter_out, parse_double_play))(input)
}

fn parse_single_batter_out(input: &str) -> IResult<&str, FieldingOutParsed, ErrorTree<&str>> {
    let (input, batter_name) = greedy_text(tag(" hit a ")).parse(input)?;
    let (input, _) = tag(" hit a ")(input)?;
    let (input, out_type) = alt((tag("flyout"), tag("ground out")))(input)?;
    let (input, _) = tag(" to ")(input)?;
    let (input, fielder_name) = greedy_text(terminated(tag("."), eof)).parse(input)?;
    let (input, _) = terminated(tag("."), eof)(input)?;

    let parsed = GroundOrFlyOutParsed {
        batter_name: batter_name.to_string(),
        fielder_name: fielder_name.to_string(),
    };

    let out = match out_type {
        "flyout" => FieldingOutParsed::Flyout(parsed),
        "ground out" => FieldingOutParsed::GroundOut(parsed),
        _ => panic!("Invalid fielding out type")
    };

    Ok((input, out))
}

fn parse_double_play(input: &str) -> IResult<&str, FieldingOutParsed, ErrorTree<&str>> {
    let (input, batter_name) = greedy_text(tag(" hit into a double play!")).parse(input)?;
    let (input, _) = tag(" hit into a double play!")(input)?;
    let (input, _) = eof(input)?;

    let parsed = DoublePlayParsed {
        batter_name: batter_name.to_string()
    };

    Ok((input, FieldingOutParsed::DoublePlay(parsed)))
}

fn parse_fielders_choice<'i>(input1: &'i str, input2: &'i str) -> IResult<&'i str, FieldingOutParsed, ErrorTree<&'i str>> {
    let (input1, runner_out_name) = greedy_text(tag(" out at ")).parse(input1)?;
    let (input1, _) = tag(" out at ")(input1)?;
    let (input1, runner_out_base) = parse_base(input1)?;
    let (input1, _) = terminated(tag(" base."), eof)(input1)?;

    assert_eq!(input1, "");

    let (input2, batter_name) = greedy_text(terminated(tag(" reaches on fielder's choice."), eof)).parse(input2)?;
    let (input2, _) = terminated(tag(" reaches on fielder's choice."), eof)(input2)?;

    let parsed = FieldersChoiceParsed {
        batter_name: batter_name.to_string(),
        runner_out_name: runner_out_name.to_string(),
        runner_out_base
    };

    Ok((input2, FieldingOutParsed::FieldersChoice(parsed)))
}

#[derive(Serialize, Deserialize)]
pub struct GroundOut {
    game_update: GamePitch,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: GroundOrFlyOutParsed,
}

impl GroundOut {
    pub fn from_parsed(feed_event: &EventuallyEvent, parsed: GroundOrFlyOutParsed) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("GroundOut event must have a game id");

        let event = Self {
            game_update: GamePitch::parse(feed_event),
            time,
            parsed,
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::GroundOut(event), effects))
    }
}

impl Event for GroundOut {
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
            other => panic!("GroundOut event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}

#[derive(Serialize, Deserialize)]
pub struct Flyout {
    game_update: GamePitch,
    time: DateTime<Utc>,
    #[serde(flatten)]
    parsed: GroundOrFlyOutParsed,
}

impl Flyout {
    pub fn from_parsed(feed_event: &EventuallyEvent, parsed: GroundOrFlyOutParsed) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;
        let game_id = feed_event.game_id().expect("Flyout event must have a game id");

        let event = Self {
            game_update: GamePitch::parse(feed_event),
            time,
            parsed,
        };

        let effects = vec![(
            "game".to_string(),
            Some(game_id),
            serde_json::Value::Null
        )];

        Ok((AnyEvent::Flyout(event), effects))
    }
}

impl Event for Flyout {
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
            other => panic!("Flyout event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}