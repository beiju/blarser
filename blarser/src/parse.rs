use anyhow::anyhow;
use nom::{IResult, error::VerboseError, bytes::complete::{tag, take_until}, character::complete::digit1, branch::alt, combinator::eof, Finish};
use crate::entity::Base;

#[derive(Copy, Clone)]
#[repr(i64)]
pub enum BaseSteal {
    Steal(Base),
    CaughtStealing(Base),
}

// pub fn parse_strikeout(batter_name: &str, input: &str) -> Result<StrikeType, anyhow::Error> {
//     strikeout(batter_name)(input)
//         .finish()
//         .map_err(|err| anyhow!("Couldn't parse strikeout: {}", err))
//         .map(|(_, result)| result)
// }
//
// fn strikeout(batter_name: &str) -> impl Fn(&str) -> IResult<&str, StrikeType, VerboseError<&str>> + '_ {
//     |input| {
//         let (input, _) = tag(batter_name.as_bytes())(input)?;
//         let (input, _) = tag(" strikes out ")(input)?;
//         let (input, out_type) = strike_type(input)?;
//         let (input, _) = tag(".")(input)?;
//         let (input, _) = eof(input)?;
//
//         IResult::Ok((input, out_type))
//     }
// }

pub fn parse_hit(batter_name: &str, input: &str) -> Result<Base, anyhow::Error> {
    hit(batter_name)(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse hit: {}", err))
        .map(|(_, result)| result)
}

fn hit(batter_name: &str) -> impl Fn(&str) -> IResult<&str, Base, VerboseError<&str>> + '_ {
    |input| {
        let (input, _) = tag(batter_name.as_bytes())(input)?;
        let (input, _) = tag(" hits a ")(input)?;
        let (input, base) = hit_base(input)?;
        let (input, _) = tag("!")(input)?;
        let (input, _) = eof(input)?;

        IResult::Ok((input, base))
    }
}

fn hit_base(input: &str) -> IResult<&str, Base, VerboseError<&str>> {
    let (input, hit_name) = alt((tag("Single"), tag("Double"), tag("Triple")))(input)?;

    IResult::Ok((input, Base::from_hit(hit_name)))
}

pub fn parse_stolen_base(thief_name: &str, input: &str) -> Result<BaseSteal, anyhow::Error> {
    alt((stolen_base(thief_name), caught_stealing(thief_name)))(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse stolen base: {}", err))
        .map(|(_, result)| result)
}

fn stolen_base(thief_name: &str) -> impl Fn(&str) -> IResult<&str, BaseSteal, VerboseError<&str>> + '_ {
    |input| {
        let (input, _) = tag(thief_name.as_bytes())(input)?;
        let (input, _) = tag(" steals ")(input)?;
        let (input, which_base) = parse_base(input)?;
        let (input, _) = tag(" base!")(input)?;
        let (input, _) = eof(input)?;

        IResult::Ok((input, BaseSteal::Steal(which_base)))
    }
}

fn caught_stealing(thief_name: &str) -> impl Fn(&str) -> IResult<&str, BaseSteal, VerboseError<&str>> + '_ {
    |input| {
        let (input, _) = tag(thief_name.as_bytes())(input)?;
        let (input, _) = tag(" gets caught stealing ")(input)?;
        let (input, which_base) = parse_base(input)?;
        let (input, _) = tag(" base.")(input)?;
        let (input, _) = eof(input)?;

        IResult::Ok((input, BaseSteal::CaughtStealing(which_base)))
    }
}

fn parse_base(input: &str) -> IResult<&str, Base, VerboseError<&str>> {
    let (input, base_name) = alt((tag("first"), tag("second"), tag("third"), tag("fourth")))(input)?;

    IResult::Ok((input, Base::from_string(base_name)))
}

pub fn parse_home_run(batter_name: &str, input: &str) -> Result<i64, anyhow::Error> {
    home_run(batter_name)(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse home run: {}", err))
        .map(|(_, result)| result)
}

fn home_run(batter_name: &str) -> impl Fn(&str) -> IResult<&str, i64, VerboseError<&str>> + '_ {
    |input| {
        let (input, _) = tag(batter_name.as_bytes())(input)?;
        let (input, _) = tag(" hits a ")(input)?;
        let (input, home_run_type) = home_run_type(input)?;
        let (input, _) = eof(input)?;

        IResult::Ok((input, home_run_type))
    }
}

fn home_run_type(input: &str) -> IResult<&str, i64, VerboseError<&str>> {
    let (input, home_run_type_name) = alt((tag("solo home run!"),
                                           tag("2-run home run!"),
                                           tag("3-run home run!"),
                                           tag("grand slam!")))(input)?;

    let result = match home_run_type_name {
        "solo home run!" => 1,
        "2-run home run!" => 2,
        "3-run home run!" => 3,
        "grand slam!" => 4,
        _ => panic!("Invalid home_run type {}", home_run_type_name)
    };

    IResult::Ok((input, result))
}

pub fn parse_snowfall(input: &str) -> Result<(i32, &str), anyhow::Error> {
    snowfall(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse snowfall: {}", err))
        .map(|(_, result)| result)
}

fn snowfall(input: &str) -> IResult<&str, (i32, &str), VerboseError<&str>> {
    let (input, num_snowflakes) = digit1(input)?;
    let (input, _) = tag(" Snowflakes ")(input)?;
    let (input, modified_type) = alt((tag("slightly modified"), tag("modified"), tag("greatly modified")))(input)?;
    let (input, _) = tag(" the field!")(input)?;
    let (input, _) = eof(input)?;


    let num_snowflakes: i32 = num_snowflakes.parse()
        .expect("Can't parse number of snowflakes: {}");


    IResult::Ok((input, (num_snowflakes, modified_type)))
}