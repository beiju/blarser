use anyhow::anyhow;
use nom::{IResult, error::VerboseError, bytes::complete::{tag, take_until}, character::complete::digit1, branch::alt, combinator::eof, Finish};

pub enum FieldingOut<'a> {
    Flyout(&'a str),
    GroundOut(&'a str),
    FieldersChoice(&'a str, Base),
    DoublePlay,
}

pub enum StrikeType {
    Swinging,
    Looking,
}

impl StrikeType {
    fn from_string(name: &str) -> Self {
        match name {
            "swinging" => { StrikeType::Swinging }
            "looking" => { StrikeType::Looking }
            _ => { panic!("Invalid strike type {}", name) }
        }
    }
}


#[derive(Copy, Clone)]
#[repr(i64)]
pub enum Base {
    First = 0,
    Second = 1,
    Third = 2,
    Fourth = 3,
}


#[derive(Copy, Clone)]
#[repr(i64)]
pub enum BaseSteal {
    Steal(Base),
    CaughtStealing(Base),
}

impl Base {
    pub fn name(&self) -> &'static str {
        match self {
            Base::First => { "first" }
            Base::Second => { "second" }
            Base::Third => { "third" }
            Base::Fourth => { "fourth" }
        }
    }

    pub fn from_string(base_name: &str) -> Self {
        match base_name {
            "first" => { Base::First }
            "second" => { Base::Second }
            "third" => { Base::Third }
            "fourth" => { Base::Fourth }
            _ => { panic!("Invalid base name {}", base_name) }
        }
    }

    pub fn from_hit(hit_name: &str) -> Self {
        match hit_name {
            "Single" => Base::First,
            "Double" => Base::Second,
            "Triple" => Base::Third,
            "Quadruple" => Base::Fourth,
            _ => panic!("Invalid hit type {}", hit_name)
        }
    }
}

pub fn parse_simple_out<'input>(batter_name: &'input str, input: &'input str) -> Result<FieldingOut<'input>, anyhow::Error> {
    alt((single_batter_out(batter_name), double_play(batter_name)))(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse simple fielding out: {}", err))
        .map(|(_, result)| result)
}

fn single_batter_out<'i>(batter_name: &'i str) -> impl Fn(&'i str) -> IResult<&'i str, FieldingOut, VerboseError<&'i str>> {
    |input| {
        let (input, _) = tag(batter_name.as_bytes())(input)?;
        let (input, _) = tag(" hit a ")(input)?;
        let (input, out_type) = alt((tag("flyout"), tag("ground out")))(input)?;
        let (input, _) = tag(" to ")(input)?;
        let (input, fielder_name) = take_until(".")(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, _) = eof(input)?;

        let out = match out_type {
            "flyout" => FieldingOut::Flyout(fielder_name),
            "ground out" => FieldingOut::GroundOut(fielder_name),
            _ => panic!("Invalid fielding out type")
        };

        Ok((input, out))
    }
}

fn double_play<'i>(batter_name: &'i str) -> impl Fn(&'i str) -> IResult<&'i str, FieldingOut, VerboseError<&'i str>> {
    |input| {
        let (input, _) = tag(batter_name.as_bytes())(input)?;
        let (input, _) = tag(" hit into a double play!")(input)?;
        let (input, _) = eof(input)?;

        Ok((input, FieldingOut::DoublePlay))
    }
}

pub fn parse_complex_out<'input>(batter_name: &'input str, input1: &'input str, input2: &'input str) -> Result<FieldingOut<'input>, anyhow::Error> {
    fielders_choice(batter_name)((input1, input2))
        .finish()
        .map_err(|err| anyhow!("Couldn't parse complex fielding out: {}", err))
        .map(|(_, result)| result)
}

fn fielders_choice<'i>(batter_name: &'i str) -> impl Fn((&'i str, &'i str)) -> IResult<(&'i str, &'i str), FieldingOut, VerboseError<&'i str>> {
    |(input1, input2)| {
        let (input1, runner_name) = take_until(" out at ")(input1)?;
        let (input1, _) = tag(" out at ")(input1)?;
        let (input1, base) = parse_base(input1)?;
        let (input1, _) = tag(" base.")(input1)?;
        let (input1, _) = eof(input1)?;

        let (input2, _) = tag(batter_name.as_bytes())(input2)?;
        let (input2, _) = tag(" reaches on fielder's choice.")(input2)?;
        let (input2, _) = eof(input2)?;

        let out = FieldingOut::FieldersChoice(runner_name, base);

        Ok(((input1, input2), out))
    }
}

pub fn parse_strikeout(batter_name: &str, input: &str) -> Result<StrikeType, anyhow::Error> {
    strikeout(batter_name)(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse strikeout: {}", err))
        .map(|(_, result)| result)
}

fn strikeout(batter_name: &str) -> impl Fn(&str) -> IResult<&str, StrikeType, VerboseError<&str>> + '_ {
    |input| {
        let (input, _) = tag(batter_name.as_bytes())(input)?;
        let (input, _) = tag(" strikes out ")(input)?;
        let (input, out_type) = strike_type(input)?;
        let (input, _) = tag(".")(input)?;
        let (input, _) = eof(input)?;

        IResult::Ok((input, out_type))
    }
}

fn strike_type(input: &str) -> IResult<&str, StrikeType, VerboseError<&str>> {
    let (input, strike_type_name) = alt((tag("looking"), tag("swinging")))(input)?;

    IResult::Ok((input, StrikeType::from_string(strike_type_name)))
}

pub fn parse_strike(input: &str) -> Result<StrikeType, anyhow::Error> {
    strike(input)
        .finish()
        .map_err(|err| anyhow!("Couldn't parse strike: {}", err))
        .map(|(_, result)| result)
}


fn strike(input: &str) -> IResult<&str, StrikeType, VerboseError<&str>> {
    let (input, _) = tag("Strike, ")(input)?;
    let (input, strike_type) = alt((tag("looking"), tag("swinging")))(input)?;
    let (input, _) = tag(". ")(input)?;
    let (input, _) = digit1(input)?;
    let (input, _) = tag("-")(input)?;
    let (input, _) = digit1(input)?;
    let (input, _) = eof(input)?;

    IResult::Ok((input, StrikeType::from_string(strike_type)))
}


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