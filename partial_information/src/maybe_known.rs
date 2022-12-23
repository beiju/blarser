use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::PartialInformationCompare;

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MaybeKnown<UnderlyingType: PartialInformationCompare> {
    #[default] Unknown,
    Known(UnderlyingType),
    UnknownExcluding(UnderlyingType::Raw),
}

impl<T: PartialInformationCompare> Copy for MaybeKnown<T> where T: Copy, T::Raw: Copy {}

impl<UnderlyingType: PartialInformationCompare> MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    pub fn known(&self) -> Option<&UnderlyingType> {
        match self {
            MaybeKnown::Unknown => { None }
            MaybeKnown::Known(val) => { Some(val) }
            MaybeKnown::UnknownExcluding(_) => { None }
        }
    }
}

impl<UnderlyingType: PartialInformationCompare> From<UnderlyingType> for MaybeKnown<UnderlyingType>
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
          T::Raw: Clone + Default + PartialEq {
    type Raw = T::Raw;
    type Diff<'d> = MaybeKnownDiff<'d, T>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        match self {
            MaybeKnown::Unknown => { MaybeKnownDiff::NoDiff }
            MaybeKnown::Known(expected) => {
                MaybeKnownDiff::Diff(expected.diff(observed, time))
            }
            MaybeKnown::UnknownExcluding(_) => { todo!() }
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
            MaybeKnown::UnknownExcluding(excluded) => {
                if excluded == observed {
                    vec![Conflict::new(String::new(),
                                       format!("Observed the excluded value {:?}", excluded))]
                } else {
                    *self = MaybeKnown::Known(T::from_raw((*observed).clone()));
                    vec![]
                }
            }
        }
    }

    fn is_ambiguous(&self) -> bool {
        match self {
            MaybeKnown::Unknown => { true }
            MaybeKnown::Known(_) => { false }
            MaybeKnown::UnknownExcluding(_) => { true }
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