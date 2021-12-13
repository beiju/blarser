use anyhow::anyhow;
use nom::{IResult, error::VerboseError, bytes::complete::{tag, take_until}, character::complete::digit1, branch::alt, combinator::eof, Finish};

pub enum FieldingOutType {
    Flyout,
    GroundOut,
}

pub enum StrikeType {
    Swinging,
    Looking,
}


#[derive(Copy, Clone)]
#[repr(i64)]
pub enum Base {
    First = 0,
    Second = 1,
    Third = 2,
    Fourth = 3,
}

impl Base {
    pub fn name(&self) -> &'static str {
        match self {
            Base::First => { "first" }
            Base::Second => { "second" }
            Base::Third => { "third" }
            Base::Fourth => { "fourth " }
        }
    }
}

pub fn parse_fielding_out<'input>(batter_name: &str, input: &'input str) -> Result<(FieldingOutType, &'input str), anyhow::Error> {
    let (_, (out_type, fielder_name)) = parse_fielding_out_internal(batter_name, input).finish()
        .map_err(|err| anyhow!("Can't parse fielding out: {}", err))?;

    let out_type = match out_type {
        "flyout" => FieldingOutType::Flyout,
        "ground out" => FieldingOutType::GroundOut,
        _ => panic!("Invalid ground out type {}", out_type)
    };

    Ok((out_type, fielder_name))
}

fn parse_fielding_out_internal<'input>(batter_name: &str, input: &'input str) -> IResult<&'input str, (&'input str, &'input str), VerboseError<&'input str>> {
    let (input, _) = tag(batter_name.as_bytes())(input)?;
    let (input, _) = tag(" hit a ")(input)?;
    let (input, out_type) = alt((tag("flyout"), tag("ground out")))(input)?;
    let (input, _) = tag(" to ")(input)?;
    let (input, fielder_name) = take_until(".")(input)?;
    let (input, _) = tag(".")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, (out_type, fielder_name)))
}

pub fn parse_strikeout(batter_name: &str, input: &str) -> Result<StrikeType, anyhow::Error> {
    let (_, out_type) = parse_strikeout_internal(batter_name, input).finish()
        .map_err(|err| anyhow!("Can't parse strikeout: {}", err))?;


    let out_type = match out_type {
        "swinging" => StrikeType::Swinging,
        "looking" => StrikeType::Looking,
        _ => panic!("Invalid strikeout type {}", out_type)
    };

    Ok(out_type)
}

fn parse_strikeout_internal<'input>(batter_name: &str, input: &'input str) -> IResult<&'input str, &'input str, VerboseError<&'input str>> {
    let (input, _) = tag(batter_name.as_bytes())(input)?;
    let (input, _) = tag(" strikes out ")(input)?;
    let (input, out_type) = alt((tag("looking"), tag("swinging")))(input)?;
    let (input, _) = tag(".")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, out_type))
}

pub fn parse_strike(input: &str) -> Result<StrikeType, anyhow::Error> {
    let (_, strike_type) = parse_strike_internal(input).finish()
        .map_err(|err| anyhow!("Can't parse strike: {}", err))?;


    let strike_type = match strike_type {
        "swinging" => StrikeType::Swinging,
        "looking" => StrikeType::Looking,
        _ => panic!("Invalid strike type {}", strike_type)
    };

    Ok(strike_type)
}

fn parse_strike_internal(input: &str) -> IResult<&str, &str, VerboseError<&str>> {
    let (input, _) = tag("Strike, ")(input)?;
    let (input, strike_type) = alt((tag("looking"), tag("swinging")))(input)?;
    let (input, _) = tag(". ")(input)?;
    let (input, _) = digit1(input)?;
    let (input, _) = tag("-")(input)?;
    let (input, _) = digit1(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, strike_type))
}

pub fn parse_hit(batter_name: &str, input: &str) -> Result<Base, anyhow::Error> {
    let (_, hit_type) = parse_hit_internal(batter_name, input).finish()
        .map_err(|err| anyhow!("Can't parse hit: {}", err))?;

    let hit_type = match hit_type {
        "Single" => Base::First,
        "Double" => Base::Second,
        "Triple" => Base::Third,
        _ => panic!("Invalid hit type {}", hit_type)
    };

    Ok(hit_type)
}

fn parse_hit_internal<'input>(batter_name: &str, input: &'input str) -> IResult<&'input str, &'input str, VerboseError<&'input str>> {
    let (input, _) = tag(batter_name.as_bytes())(input)?;
    let (input, _) = tag(" hits a ")(input)?;
    let (input, hit_type) = alt((tag("Single"), tag("Double"), tag("Triple")))(input)?;
    let (input, _) = tag("!")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, hit_type))
}

pub fn parse_stolen_base(thief_name: &str, input: &str) -> Result<Base, anyhow::Error> {
    let (_, which_base) = parse_stolen_base_internal(thief_name, input).finish()
        .map_err(|err| anyhow!("Can't parse stolen base: {}", err))?;

    let which_base = match which_base {
        "second" => Base::Second,
        "third" => Base::Third,
        "fourth" => Base::Fourth,
        _ => panic!("Invalid stolen base type {}", which_base)
    };

    Ok(which_base)
}

fn parse_stolen_base_internal<'input>(thief_name: &str, input: &'input str) -> IResult<&'input str, &'input str, VerboseError<&'input str>> {
    let (input, _) = tag(thief_name.as_bytes())(input)?;
    let (input, _) = tag(" steals ")(input)?;
    let (input, which_base) = alt((tag("second"), tag("third"), tag("fourth")))(input)?;
    let (input, _) = tag(" base!")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, which_base))
}

pub fn parse_home_run(batter_name: &str, input: &str) -> Result<i64, anyhow::Error> {
    let (_, home_run_type) = parse_home_run_internal(batter_name, input).finish()
        .map_err(|err| anyhow!("Can't parse home_run: {}", err))?;

    let home_run_score = match home_run_type {
        "solo" => 1,
        "2-run" => 2,
        "3-run" => 3,
        "4-run" => 4,
        _ => panic!("Invalid home_run type {}", home_run_type)
    };

    Ok(home_run_score)
}

fn parse_home_run_internal<'input>(batter_name: &str, input: &'input str) -> IResult<&'input str, &'input str, VerboseError<&'input str>> {
    let (input, _) = tag(batter_name.as_bytes())(input)?;
    let (input, _) = tag(" hits a ")(input)?;
    let (input, home_run_type) = alt((tag("solo"), tag("2-run"), tag("3-run")))(input)?;
    let (input, _) = tag(" home run!")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, home_run_type))
}

pub fn parse_snowfall(input: &str) -> Result<(i32, &str), anyhow::Error> {
    let (_, (num_snowflakes, modified_type)) = parse_snowfall_internal(input).finish()
        .map_err(|err| anyhow!("Can't parse snowfall: {}", err))?;

    let num_snowflakes = num_snowflakes.parse()
        .map_err(|err| anyhow!("Can't parse number of snowflakes: {:?}", err))?;

    Ok((num_snowflakes, modified_type))
}

fn parse_snowfall_internal(input: &str) -> IResult<&str, (&str, &str), VerboseError<&str>> {
    let (input, num_snowflakes) = digit1(input)?;
    let (input, _) = tag(" Snowflakes ")(input)?;
    let (input, modified_type) = alt((tag("slightly modified"), tag("modified"), tag("heavily modified")))(input)?;
    let (input, _) = tag(" the field!")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, (num_snowflakes, modified_type)))
}