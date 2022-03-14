use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::PartialInformationCompare;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct Spurious<UnderlyingType>(UnderlyingType);

// Spurious is Clone if underlying type is Clone
impl<UnderlyingType> Clone for Spurious<UnderlyingType>
    where UnderlyingType: Clone {
    fn clone(&self) -> Self {
        Spurious(self.0.clone())
    }
}

// Spurious is Copy if underlying type is Copy
impl<UnderlyingType> Copy for Spurious<UnderlyingType>
    where UnderlyingType: Copy + Clone {}


#[derive(Debug)]
pub enum SpuriousDiff<'d, T: 'd + PartialInformationCompare> {
    Spurious,
    Underlying(T::Diff<'d>),
}

impl<UnderlyingType> Deref for Spurious<UnderlyingType> {
    type Target = UnderlyingType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<UnderlyingType> DerefMut for Spurious<UnderlyingType> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> PartialInformationCompare for Spurious<T>
    where T: 'static + Clone + Debug + PartialOrd + for<'de> Deserialize<'de> + Serialize + Send + Sync + PartialInformationCompare,
          T::Raw: Default + PartialEq {
    type Raw = T::Raw;
    type Diff<'d> = SpuriousDiff<'d, T>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        if observed == &Self::Raw::default() {
            SpuriousDiff::Spurious
        } else {
            SpuriousDiff::Underlying(self.0.diff(observed, time))
        }
    }

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        if observed == &Self::Raw::default() {
            vec![]
        } else {
            self.0.observe(observed)
        }
    }

    fn from_raw(raw: Self::Raw) -> Self {
        Self(T::from_raw(raw))
    }
}

impl<'d, T> PartialInformationDiff<'d> for SpuriousDiff<'d, T>
    where T: PartialOrd + Debug + PartialInformationCompare {
    fn is_empty(&self) -> bool {
        match self {
            SpuriousDiff::Spurious => { true }
            SpuriousDiff::Underlying(underlying) => { underlying.is_empty() }
        }
    }
}