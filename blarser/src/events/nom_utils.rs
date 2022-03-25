use nom::error::ParseError;
use nom::Parser;
use nom::bytes::complete::{take_till1, take_while1};
use nom::combinator::{peek, recognize};
use nom_supreme::multi::collect_separated_terminated;

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
