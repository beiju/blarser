#![feature(generic_associated_types)]

mod ranged;
mod maybe_known;
mod compare;

pub use ranged::Ranged;
pub use maybe_known::MaybeKnown;
pub use compare::{PartialInformationCompare, PartialInformationDiff};
