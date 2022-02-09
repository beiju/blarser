use std::fmt::Debug;
use std::ops::Add;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use crate::compare::PartialInformationDiff;

use crate::PartialInformationCompare;

#[derive(Clone, Debug, Deserialize)]
pub struct Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    value: UnderlyingType,
    history: Vec<(UnderlyingType, DateTime<Utc>)>,
}

#[derive(Debug)]
pub struct CachedDiff<'d, T: 'd + PartialInformationCompare> {
    value_diff: T::Diff<'d>,
    history_diffs: Vec<(T::Diff<'d>, DateTime<Utc>)>,
}

impl<T> PartialInformationCompare for Cached<T>
    where T: 'static + Clone + PartialInformationCompare {
    type Raw = T::Raw;
    type Diff<'d> = CachedDiff<'d, T>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        CachedDiff {
            value_diff: self.value.diff(observed, time),
            history_diffs: self.history.iter()
                .flat_map(|(value, expiry)| {
                    if time < *expiry {
                        Some((value.diff(observed, time), *expiry))
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
}

impl<'d, T: 'd> PartialInformationDiff<'d> for CachedDiff<'d, T>
    where T: Clone + PartialInformationCompare {
    fn is_empty(&self) -> bool {
        self.value_diff.is_empty() && self.history_diffs.iter().all(|(diff, _)| diff.is_empty())
    }
}

impl<UnderlyingType> Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    pub fn set_uncached(&mut self, value: UnderlyingType) {
        self.value = value;
        self.history.clear();
    }

    pub fn update_uncached<F>(&mut self, update_fn: F)
        where F: FnOnce(&UnderlyingType) -> UnderlyingType {
        self.set_uncached(update_fn(&self.value));
    }

    pub fn set_cached(&mut self, value: UnderlyingType, deadline: DateTime<Utc>) {
        let old_val = std::mem::replace(&mut self.value, value);
        // It's deadline from the perspective of the new value and expiry from the
        // perspective of the old value
        self.history.push((old_val, deadline));
    }

    pub fn add_cached<AddT>(&mut self, to_add: AddT, deadline: DateTime<Utc>)
        where for<'a> &'a UnderlyingType: Add<AddT, Output=UnderlyingType> {
        let new_val = &self.value + to_add;
        self.set_cached(new_val, deadline);
    }
}