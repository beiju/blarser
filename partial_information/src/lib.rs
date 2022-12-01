mod rerollable;
mod maybe_known;
mod compare;
mod spurious;

pub use rerollable::Rerollable;
pub use maybe_known::MaybeKnown;
pub use compare::{PartialInformationCompare, PartialInformationDiff, Conflict};
pub use spurious::Spurious;
