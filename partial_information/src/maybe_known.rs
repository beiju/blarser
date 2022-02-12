use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use crate::compare::PartialInformationDiff;
use crate::PartialInformationCompare;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum MaybeKnown<UnderlyingType> {
    Unknown,
    Known(UnderlyingType),
}

impl<UnderlyingType> MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    pub fn known(&self) -> Option<&UnderlyingType> {
        match self {
            MaybeKnown::Unknown => { None }
            MaybeKnown::Known(val) => { Some(val) }
        }
    }
}

impl<'de, UnderlyingType> Deserialize<'de> for MaybeKnown<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(MaybeKnown::Known(val))
    }
}

impl<UnderlyingType> From<UnderlyingType> for MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    fn from(item: UnderlyingType) -> Self {
        Self::Known(item)
    }
}

#[derive(Debug)]
pub enum MaybeKnownDiff<'d, T: 'd + PartialInformationCompare> {
    NoDiff,
    Diff(T::Diff<'d>),
}

impl<T> PartialInformationCompare for MaybeKnown<T>
    where T: 'static + PartialInformationCompare {
    type Raw = T::Raw;
    type Diff<'d> = MaybeKnownDiff<'d, T>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        match self {
            MaybeKnown::Unknown => { MaybeKnownDiff::NoDiff }
            MaybeKnown::Known(expected) => {
                MaybeKnownDiff::Diff(expected.diff(observed, time))
            }
        }
    }

    fn from_raw(raw: Self::Raw) -> Self {
        MaybeKnown::Known(T::from_raw(raw))
    }
}

impl<'d, T> PartialInformationDiff<'d> for MaybeKnownDiff<'d, T>
    where T: PartialInformationCompare {
    fn is_empty(&self) -> bool {
        match self {
            MaybeKnownDiff::NoDiff => { true }
            MaybeKnownDiff::Diff(nested) => { nested.is_empty() }
        }
    }
}