use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::iter;
use uuid::Uuid;
use std::iter::Iterator;
use chrono::{DateTime, Utc};
use serde::Deserialize;

pub trait PartialInformationDiff<'d>: Debug {
    fn is_empty(&self) -> bool;
}

pub trait PartialInformationCompare: Sized + Debug {
    type Raw: for<'de> Deserialize<'de> + Debug;
    type Diff<'d>: PartialInformationDiff<'d > where Self: 'd;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d>;
}


#[derive(Debug)]
pub struct HashMapDiff<'d, KeyT, ValT: PartialInformationCompare> {
    missing: HashMap<KeyT, &'d ValT>,
    extra: HashMap<KeyT, &'d ValT::Raw>,
    common: HashMap<KeyT, ValT::Diff<'d>>,
}

impl<K, V> PartialInformationCompare for HashMap<K, V>
    where K: 'static + Eq + Hash + Clone + for<'de> Deserialize<'de> + Debug,
          V: 'static + PartialInformationCompare {
    type Raw = HashMap<K, V::Raw>;
    type Diff<'d> = HashMapDiff<'d, K, V>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        let expected_keys: HashSet<_> = self.keys().collect();
        let observed_keys: HashSet<_> = observed.keys().collect();

        HashMapDiff {
            missing: expected_keys.difference(&observed_keys)
                .map(|&key| (key.clone(), self.get(key).unwrap()))
                .collect(),
            extra: observed_keys.difference(&expected_keys)
                .map(|&key| (key.clone(), observed.get(key).unwrap()))
                .collect(),
            common: observed_keys.intersection(&expected_keys)
                .map(|&key| {
                    (key.clone(), self.get(key).unwrap().diff(observed.get(key).unwrap(), time))
                })
                .collect(),
        }
    }
}

impl<'d, K, V> PartialInformationDiff<'d> for HashMapDiff<'d, K, V>
    where K: Eq + Hash + Clone + Debug,
          V: PartialInformationCompare {
    fn is_empty(&self) -> bool {
        self.extra.is_empty() && self.missing.is_empty() && self.common.iter().all(|(_, diff)| diff.is_empty())
    }
}

#[derive(Debug)]
pub enum OptionDiff<'d, ItemT: 'd + PartialInformationCompare> {
    ExpectedNoneGotNone,
    ExpectedNoneGotSome(&'d ItemT::Raw),
    ExpectedSomeGotNone(&'d ItemT),
    ExpectedSomeGotSome(ItemT::Diff<'d>),
}

impl<T> PartialInformationCompare for Option<T>
    where T: 'static + PartialInformationCompare {
    type Raw = Option<T::Raw>;
    type Diff<'d> = OptionDiff<'d, T>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        match (self, observed) {
            (None, None) => OptionDiff::ExpectedNoneGotNone,
            (None, Some(val)) => OptionDiff::ExpectedNoneGotSome(val),
            (Some(val), None) => OptionDiff::ExpectedSomeGotNone(val),
            (Some(a), Some(b)) => OptionDiff::ExpectedSomeGotSome(a.diff(b, time))
        }
    }
}

impl<'d, T> PartialInformationDiff<'d> for OptionDiff<'d, T>
    where T: PartialInformationCompare {
    fn is_empty(&self) -> bool {
        match self {
            OptionDiff::ExpectedNoneGotNone => { true }
            OptionDiff::ExpectedNoneGotSome(_) => { false }
            OptionDiff::ExpectedSomeGotNone(_) => { false }
            OptionDiff::ExpectedSomeGotSome(diff) => { diff.is_empty() }
        }
    }
}

#[derive(Debug)]
pub struct VecDiff<'d, T: PartialInformationCompare> {
    missing: &'d [T],
    extra: &'d [T::Raw],
    common: Vec<T::Diff<'d>>,
}

impl<ItemT> PartialInformationCompare for Vec<ItemT>
    where ItemT: 'static + PartialInformationCompare {
    type Raw = Vec<ItemT::Raw>;
    type Diff<'d> = VecDiff<'d, ItemT>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
        VecDiff {
            missing: &self[observed.len()..],
            extra: &observed[self.len()..],
            common: iter::zip(self, observed)
                .map(|(self_item, other_item)| self_item.diff(other_item, time))
                .collect(),
        }
    }
}

impl<'d, T> PartialInformationDiff<'d> for VecDiff<'d, T>
    where T: PartialInformationCompare {
    fn is_empty(&self) -> bool {
        self.extra.is_empty() && self.missing.is_empty() && self.common.iter().all(|diff| diff.is_empty())
    }
}

#[derive(Debug)]
pub enum PrimitiveDiff<'d, T: Debug> {
    NoDiff,
    Diff(&'d T, &'d T),
}

impl<'d, T> PartialInformationDiff<'d> for PrimitiveDiff<'d, T>
    where T: Debug {
    fn is_empty(&self) -> bool {
        match self {
            PrimitiveDiff::NoDiff => { true }
            PrimitiveDiff::Diff(_, _) => { false }
        }
    }
}

macro_rules! trivial_compare {
    ($($t:ty),+) => {
        $(impl PartialInformationCompare for $t {
            type Raw = Self;
            type Diff<'d> = PrimitiveDiff<'d, $t>;

            fn diff<'d>(&'d self, observed: &'d $t, _: DateTime<Utc>) -> Self::Diff<'d> {
                if self.eq(observed) {
                    PrimitiveDiff::NoDiff
                } else {
                    PrimitiveDiff::Diff(self, observed)
                }
            }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);