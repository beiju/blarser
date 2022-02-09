use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter;
use uuid::Uuid;
use std::iter::Iterator;
use chrono::{DateTime, Utc};

pub trait PartialInformationDiff<'exp, 'obs> {
    fn is_empty(&self) -> bool;
}

pub trait PartialInformationCompare: Sized {
    type Raw;
    type Diff<'exp, 'obs>: PartialInformationDiff<'exp, 'obs> where Self: 'exp;

    fn diff<'exp, 'obs>(&'exp self, observed: &'obs Self::Raw, time: DateTime<Utc>) -> Self::Diff<'exp, 'obs>;
}


pub struct HashMapDiff<'exp, 'obs, KeyT, ValT: PartialInformationCompare> {
    missing: HashMap<KeyT, &'exp ValT>,
    extra: HashMap<KeyT, &'obs ValT::Raw>,
    common: HashMap<KeyT, ValT::Diff<'exp, 'obs>>,
}

impl<K, V> PartialInformationCompare for HashMap<K, V>
    where K: Eq + Hash + Clone,
          V: PartialInformationCompare {
    type Raw = HashMap<K, V::Raw>;
    type Diff<'exp, 'obs> = HashMapDiff<'exp, 'obs, K, V>;

    fn diff<'exp, 'obs>(&'exp self, observed: &'obs HashMap<K, V::Raw>, time: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
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

impl<'exp, 'obs, K, V> PartialInformationDiff<'exp, 'obs> for HashMapDiff<'exp, 'obs, K, V>
    where K: Eq + Hash + Clone,
          V: PartialInformationCompare {
    fn is_empty(&self) -> bool {
        self.extra.is_empty() && self.missing.is_empty() && self.common.iter().all(|(_, diff)| diff.is_empty())
    }
}

pub enum OptionDiff<'exp, 'obs, ItemT: 'exp + PartialInformationCompare> {
    ExpectedNoneGotNone,
    ExpectedNoneGotSome(&'obs ItemT::Raw),
    ExpectedSomeGotNone(&'exp ItemT),
    ExpectedSomeGotSome(ItemT::Diff<'exp, 'obs>),
}

impl<T> PartialInformationCompare for Option<T>
    where T: PartialInformationCompare {
    type Raw = Option<T::Raw>;
    type Diff<'exp, 'obs> = OptionDiff<'exp, 'obs, T>;

    fn diff<'exp, 'obs>(&'exp self, observed: &'obs Option<T::Raw>, time: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
        match (self, observed) {
            (None, None) => OptionDiff::ExpectedNoneGotNone,
            (None, Some(val)) => OptionDiff::ExpectedNoneGotSome(val),
            (Some(val), None) => OptionDiff::ExpectedSomeGotNone(val),
            (Some(a), Some(b)) => OptionDiff::ExpectedSomeGotSome(a.diff(b, time))
        }
    }
}

impl<'exp, 'obs, T> PartialInformationDiff<'exp, 'obs> for OptionDiff<'exp, 'obs, T>
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

pub struct VecDiff<'exp, 'obs, T: PartialInformationCompare> {
    missing: &'exp [T],
    extra: &'obs [T::Raw],
    common: Vec<T>,
}

impl<ItemT> PartialInformationCompare for Vec<ItemT>
    where ItemT: PartialInformationCompare {
    type Raw = Vec<ItemT::Raw>;
    type Diff<'exp, 'obs> = VecDiff<'exp, 'obs, ItemT>;

    fn diff<'exp, 'obs>(&'exp self, observed: &'obs Vec<ItemT::Raw>, time: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
        VecDiff {
            missing: &self[observed.len()..],
            extra: &observed[self.len()..],
            common: iter::zip(self, observed)
                .map(|(self_item, other_item)| self_item.diff(other_item, time))
                .collect(),
        }
    }
}

impl<'exp, 'obs, T> PartialInformationDiff<'exp, 'obs> for VecDiff<'exp, 'obs, T>
    where T: PartialInformationCompare {
    fn is_empty(&self) -> bool {
        self.extra.is_empty() && self.missing.is_empty() && self.common.iter().all(|diff| diff.is_empty())
    }
}

enum PrimitiveDiff<'exp, 'obs, T> {
    NoDiff,
    Diff(&'exp T, &'obs T),
}

impl<'exp, 'obs, T> PartialInformationDiff<'exp, 'obs> for PrimitiveDiff<'exp, 'obs, T> {
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
            type Diff<'exp, 'obs> = PrimitiveDiff<'exp, 'obs, $t>;

            fn diff<'exp, 'obs>(&'exp self, observed: &'obs $t, time: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
                if self.eq(observed) {
                    PrimitiveDiff::NoDiff
                } else {
                    PrimitiveDiff::Diff((self, observed))
                }
            }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);