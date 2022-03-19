use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::PartialInformationCompare;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    where T: 'static + PartialInformationCompare,
          T::Raw: Clone + Default {
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

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        match self {
            MaybeKnown::Unknown => {
                *self = MaybeKnown::Known(T::from_raw((*observed).clone()));
                vec![]
            }
            MaybeKnown::Known(expected) => {
                expected.observe(observed)
            }
        }
    }

    fn from_raw(raw: Self::Raw) -> Self {
        MaybeKnown::Known(T::from_raw(raw))
    }
    fn raw_approximation(self) -> Self::Raw {
        Self::Raw::default()
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