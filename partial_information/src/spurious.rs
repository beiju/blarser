use std::fmt::Debug;
use std::ops::{AddAssign, SubAssign};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use crate::PartialInformationCompare;

// A wrapper for properties that sometimes return spurious values, like Team.win_streak. For now,
// assumes the spurious value is Default::default(). This is hard to change without generic-type
// const generics.
#[derive(Debug, Clone)]
pub struct Spurious<UnderlyingType>(UnderlyingType)
    where UnderlyingType: Clone + Debug + Default + PartialOrd;

impl<'de, UnderlyingType> Deserialize<'de> for Spurious<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug + Default + PartialOrd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(Spurious(val))
    }
}

impl<T> AddAssign<T> for Spurious<T>
    where T: AddAssign<T> + Clone + Debug + Default + PartialOrd + PartialInformationCompare {
    fn add_assign(&mut self, rhs: T) {
        self.0 += rhs
    }
}

impl<T> SubAssign<T> for Spurious<T>
    where T: SubAssign<T> + Clone + Debug + Default + PartialOrd + PartialInformationCompare {
    fn sub_assign(&mut self, rhs: T) {
        self.0 -= rhs
    }
}

impl<T> PartialInformationCompare for Spurious<T>
    where T: Clone + Debug + Default + PartialOrd + PartialInformationCompare {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        if other.0.eq(&Default::default()) {
            // If the other value is equal to the default, it's either a spurious value or it's
            // correct. Either way, no conflicts.
            None
        } else {
            // Otherwise, delegate
            self.0.get_conflicts_internal(&other.0, time, field_path)
        }
    }
}