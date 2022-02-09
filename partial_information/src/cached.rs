use std::fmt::Debug;
use std::ops::Add;
use chrono::{DateTime, Utc};
use crate::compare::PartialInformationDiff;

use crate::PartialInformationCompare;

#[derive(Clone)]
pub struct Cached<UnderlyingType>
    where UnderlyingType: Clone {
    value: UnderlyingType,
    history: Vec<(UnderlyingType, DateTime<Utc>)>,
}

pub struct CachedDiff<'exp, 'obs, T: PartialInformationCompare> {
    value_diff: T::Diff<'exp, 'obs>,
    history_diffs: Vec<(T::Diff<'exp, 'obs>, DateTime<Utc>)>,
}

impl<T> PartialInformationCompare for Cached<T>
    where T: Clone + PartialInformationCompare {
    type Raw = T::Raw;
    type Diff<'exp, 'obs> = CachedDiff<'exp, 'obs, T>;

    fn diff<'exp, 'obs>(&'exp self, observed: &'obs T, time: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
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

impl<'exp, 'obs, T: 'exp> PartialInformationDiff<'exp, 'obs> for CachedDiff<'exp, 'obs, T>
    where T: Clone + PartialInformationCompare {
    fn is_empty(&self) -> bool {
        self.value_diff.is_empty() && self.history_diffs.iter().all(|diff| diff.is_empty())
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