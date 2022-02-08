use std::fmt::Debug;
use std::ops::Add;
use chrono::{DateTime, Utc};

use crate::PartialInformationCompare;

#[derive(Debug, Clone)]
pub struct Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    value: UnderlyingType,
    history: Vec<(UnderlyingType, DateTime<Utc>)>,
}

pub struct CachedDiff<'exp, 'obs, T: PartialInformationCompare<'exp, 'obs>> {
    value_diff: T::Diff,
    history_diffs: Vec<(T::Diff, DateTime<Utc>)>,
}

impl<'exp, 'obs, T> PartialInformationCompare<'exp, 'obs> for Cached<T>
    where T: Clone + Debug + PartialInformationCompare<'exp, 'obs> {
    type Raw = T::Raw;
    type Diff = CachedDiff<'exp, 'obs, T>;

    fn diff(&'exp self, other: &'obs Self::Raw, time: DateTime<Utc>) -> Self::Diff {
        CachedDiff {
            value_diff: self.value.diff(other, time),
            history_diffs: self.history.iter()
                .flat_map(|(value, expiry)| {
                    if time < *expiry {
                        Some((value.diff(other, time), *expiry))
                    } else {
                        None
                    }
                })
                .collect()
        }
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