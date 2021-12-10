use nom::{IResult, error::VerboseError, bytes::complete::{tag, take_until}, character::complete::digit1, branch::alt, combinator::eof, Finish};

pub enum FieldingOutType {
    Flyout,
    GroundOut,
}

pub enum StrikeType {
    Swinging,
    Looking,
}


pub enum HitType {
    Single = 0,
    Double = 1,
    Triple = 2,
}

pub fn parse_ground_out<'input>(batter_name: &str, input: &'input str) -> Result<(FieldingOutType, &'input str), VerboseError<&'input str>> {
    let (_, (out_type, fielder_name)) = parse_ground_out_internal(batter_name, input).finish()?;

    let out_type = match out_type {
        "flyout" => FieldingOutType::Flyout,
        "ground out" => FieldingOutType::GroundOut,
        _ => panic!("Invalid ground out type {}", out_type)
    };

    Ok((out_type, fielder_name))
}

fn parse_ground_out_internal<'input>(batter_name: &str, input: &'input str) -> IResult<&'input str, (&'input str, &'input str), VerboseError<&'input str>> {
    let (input, _) = tag(batter_name.as_bytes())(input)?;
    let (input, _) = tag(" hit a ")(input)?;
    let (input, out_type) = alt((tag("flyout"), tag("ground out")))(input)?;
    let (input, _) = tag(" to ")(input)?;
    let (input, fielder_name) = take_until(".")(input)?;
    let (input, _) = tag(".")(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, (out_type, fielder_name)))
}

pub fn parse_strikeout<'input>(batter_name: &str, input: &'input str) -> Result<StrikeType, VerboseError<&'input str>> {
    let (_, out_type) = parse_strikeout_internal(batter_name, input).finish()?;

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

pub fn parse_strike(input: &str) -> Result<StrikeType, VerboseError<&str>> {
    let (_, strike_type) = parse_strike_internal(input).finish()?;

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

pub fn parse_hit<'input>(batter_name: &str, input: &'input str) -> Result<HitType, VerboseError<&'input str>> {
    let (_, hit_type) = parse_hit_internal(batter_name, input).finish()?;

    let hit_type = match hit_type {
        "Single" => HitType::Single,
        "Double" => HitType::Double,
        "Triple" => HitType::Triple,
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