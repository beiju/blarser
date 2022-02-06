mod ranged;
mod maybe_known;
mod compare;
mod delayed_update;
mod spurious;

pub use ranged::Ranged;
pub use maybe_known::MaybeKnown;
pub use delayed_update::DelayedUpdateMap;
pub use spurious::Spurious;
pub use compare::{PartialInformationCompare};