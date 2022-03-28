use anyhow::anyhow;
use nom::{IResult, error::VerboseError, bytes::complete::{tag, take_until}, character::complete::digit1, branch::alt, combinator::eof, Finish};
use crate::entity::Base;

#[derive(Copy, Clone)]
#[repr(i64)]
pub enum BaseSteal {
    Steal(Base),
    CaughtStealing(Base),
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

