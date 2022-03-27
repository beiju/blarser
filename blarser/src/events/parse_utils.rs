use itertools::Itertools;
use nom::error::ParseError;
use nom::{IResult, Parser};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_till1, take_while1};
use nom::combinator::{peek, recognize};
use nom_supreme::error::ErrorTree;
use nom_supreme::multi::collect_separated_terminated;
use crate::api::{EventType, EventuallyEvent};
use crate::entity::Base;

pub struct RunsScored<'e> {
    pub players: &'e [EventuallyEvent],
    pub team: &'e EventuallyEvent,
}

pub struct CollatedEvents<'e> {
    pub shame: Option<&'e EventuallyEvent>,
    pub action: &'e [EventuallyEvent],
    pub score: Option<RunsScored<'e>>,
}

// This function collates the siblings of an event into shaming event (if any), "action events"
// (which is my term for the actual thing the batter, or other actor, did), then scoring events (if
// any). It will probably expand in future to include a zillion things like hype, balloons, etc.)
pub fn collate_siblings(events: &[EventuallyEvent]) -> CollatedEvents {
    let first = events.first()
        .expect("All events must have at least one sibling");

    // If there is a ShamingRun event, it's first
    let (shame, events) = if first.r#type == EventType::ShamingRun {
        events.split_first()
            .map(|(first, rest)| (Some(first), rest))
            .unwrap()
    } else {
        (None, events)
    };

    // After the ShamingRun event is the event that defines which action it is
    let action_type = events.first()
        .expect("Siblings array is missing an action-defining event")
        .r#type;

    let (action, events) = if action_type == EventType::Hit || action_type == EventType::Walk {
        // Hit and Walk events have only one action event. They need to be separated out this way
        // because scoring events reuse the Hit and Walk event type
        events.split_at(1)
    } else {
        // Other action events are delineated by the next scoring event (which seems to always use
        // the Hit type, if we took this branch
        let after_last_action = events.iter()
            .find_position(|event| event.r#type == EventType::Hit)
            .map(|(i, _)| i)
            .unwrap_or(events.len());

        events.split_at(after_last_action)
    };

    let (score, events) = if events.len() > 0 {
        // If there are events at this point, they must be "player scored" events, and they continue
        // until the next TeamRunsScored event
        let (after_last_score, _) = events.iter()
            .find_position(|event| event.r#type == EventType::RunsScored)
            .expect("Expected this event to have a RunsScored sibling");
        let (players, events) = events.split_at(after_last_score);
        let (team, events) = events.split_first().unwrap();

        (Some(RunsScored { players, team }), events)
    } else {
        (None, events)
    };

    assert!(events.is_empty(), "Unexpected sibling in collate_siblings");

    CollatedEvents {
        shame,
        action,
        score,
    }
}

pub fn parse_base(input: &str) -> IResult<&str, Base, ErrorTree<&str>> {
    let (input, base_name) = alt((
        tag("first"),
        tag("second"),
        tag("third"),
        tag("fourth"),
    ))(input)?;

    Ok((input, Base::from_string(base_name)))
}

// Split greedy text on any character that might be the end of the string: whitespace, newline,
// period, apostrophe, anything else I think of later. These can be inside the string, but they
// denote places we'll start looking for
fn greedy_text_split(c: char) -> bool {
    match c {
        '.' | '\'' => true,
        c if c.is_whitespace() => true,
        _ => false
    }
}

struct NilExtend;

impl Default for NilExtend {
    fn default() -> Self { Self }
}

impl<'l> Extend<&'l str> for NilExtend {
    fn extend<T: IntoIterator<Item=&'l str>>(&mut self, _: T) {}
}

// This function only exists because this is the only way I can find to make the compiler infer
// NilExtend for the Collect type of collect_separated_terminated
fn greedy_text_helper<'input, P, F, E: ParseError<&'input str>>(
    terminator: F,
) -> impl Parser<&'input str, NilExtend, E>
    where
        F: Parser<&'input str, P, E>,
{
    collect_separated_terminated(
        take_till1(greedy_text_split),
        take_while1(greedy_text_split),
        peek(terminator),
    )
}

pub fn greedy_text<'input, P, F, E: ParseError<&'input str>>(
    terminator: F,
) -> impl Parser<&'input str, &'input str, E>
    where
        F: Parser<&'input str, P, E>,
{
    recognize(greedy_text_helper(terminator))
}
