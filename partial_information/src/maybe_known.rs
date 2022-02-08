use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use crate::PartialInformationCompare;

#[derive(Clone, Debug)]
pub enum MaybeKnown<UnderlyingType: Clone + Debug + PartialOrd> {
    Unknown,
    Known(UnderlyingType),
}

impl<UnderlyingType> MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug + PartialOrd {
    pub fn known(&self) -> Option<&UnderlyingType> {
        match self {
            MaybeKnown::Unknown => { None }
            MaybeKnown::Known(val) => { Some(val) }
        }
    }
}

impl<'de, UnderlyingType> Deserialize<'de> for MaybeKnown<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug + PartialOrd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(MaybeKnown::Known(val))
    }
}

impl<UnderlyingType> From<UnderlyingType> for MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug + PartialOrd {
    fn from(item: UnderlyingType) -> Self {
        Self::Known(item)
    }
}

impl<T> PartialInformationCompare for MaybeKnown<T>
    where T: Clone + Debug + PartialOrd + for<'de> Deserialize<'de> {
    type Raw = T;

    fn get_conflicts_internal(&self, other: &T, _: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
        (match self {
            MaybeKnown::Unknown => { None }
            MaybeKnown::Known(expected) => {
                if expected == other {
                    None
                } else {
                    Some(format!("- {}: Expected {:?}, but value was {:?}", field_path, expected, other))
                }
            }
        }, true) // MaybeKnown is always canonical
    }
}