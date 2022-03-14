#![feature(generic_associated_types)]

mod ranged;
mod maybe_known;
mod compare;
mod spurious;

pub use ranged::Ranged;
pub use maybe_known::MaybeKnown;
pub use compare::{PartialInformationCompare, PartialInformationDiff, Conflict};
pub use spurious::Spurious;
