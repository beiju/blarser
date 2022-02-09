use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use crate::PartialInformationCompare;

#[derive(Clone, Debug)]
pub enum MaybeKnown<UnderlyingType: PartialOrd> {
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

enum MaybeKnownDiff<'exp, 'obs, T: PartialInformationCompare> {
    NoDiff,
    Diff(T::Diff<'exp, 'obs>)
}

impl<T> PartialInformationCompare for MaybeKnown<T>
    where T: PartialOrd + PartialInformationCompare {
    type Raw = T;
    type Diff<'exp, 'obs> = MaybeKnownDiff<'exp, 'obs, T>;

    fn diff<'exp, 'obs>(&'exp self, observed: &'obs T, _: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
        match self {
            MaybeKnown::Unknown => { MaybeKnownDiff::NoDiff }
            MaybeKnown::Known(expected) => {
                if expected == observed {
                    MaybeKnownDiff::NoDiff
                } else {
                    MaybeKnownDiff::Diff((expected, observed))
                }
            }
        }
    }
}