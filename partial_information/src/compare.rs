use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter;
use uuid::Uuid;
use std::iter::Iterator;
use chrono::{DateTime, Utc};

pub trait PartialInformationCompare<'exp, 'obs> {
    type Raw;
    type Diff;

    fn diff(&'exp self, other: &'obs Self::Raw, time: DateTime<Utc>) -> Self::Diff;
}

pub struct HashMapDiff<'exp, 'obs, KeyT, ValT: PartialInformationCompare<'exp, 'obs>> {
    missing: HashMap<KeyT, &'exp ValT>,
    extra: HashMap<KeyT, &'obs ValT::Raw>,
    common: HashMap<KeyT, ValT::Diff>,
}

impl<'exp, 'obs, K, V> PartialInformationCompare<'exp, 'obs> for HashMap<K, V>
    where K: 'exp + 'obs + Eq + Hash + Clone,
          V: 'exp + PartialInformationCompare<'exp, 'obs>,
          V::Raw: 'obs {
    type Raw = HashMap<K, V::Raw>;
    type Diff = HashMapDiff<'exp, 'obs, K, V>;

    fn diff(&'exp self, other: &'obs Self::Raw, time: DateTime<Utc>) -> Self::Diff {
        let self_keys: HashSet<_> = self.keys().collect();
        let other_keys: HashSet<_> = other.keys().collect();

        HashMapDiff {
            missing: self_keys.difference(&other_keys)
                .map(|&key| (key.clone(), self.get(key).unwrap()))
                .collect(),
            extra: other_keys.difference(&self_keys)
                .map(|&key| (key.clone(), other.get(key).unwrap()))
                .collect(),
            common: other_keys.intersection(&self_keys)
                .map(|&key| {
                    (key.clone(), self.get(key).unwrap().diff(other.get(key).unwrap(), time))
                })
                .collect(),
        }
    }
}

pub enum OptionDiff<'exp, 'obs, ItemT: 'exp + PartialInformationCompare<'exp, 'obs>> {
    ExpectedNoneGotNone,
    ExpectedNoneGotSome(&'obs ItemT::Raw),
    ExpectedSomeGotNone(&'exp ItemT),
    ExpectedSomeGotSome(ItemT::Diff),
}

impl<'exp, 'obs, ItemT> PartialInformationCompare<'exp, 'obs> for Option<ItemT>
    where ItemT: 'exp + PartialInformationCompare<'exp, 'obs>,
          ItemT::Raw: 'obs {
    type Raw = Option<ItemT::Raw>;
    type Diff = OptionDiff<'exp, 'obs, ItemT>;

    fn diff(&'exp self, other: &'obs Self::Raw, time: DateTime<Utc>) -> Self::Diff {
        match (self, other) {
            (None, None) => OptionDiff::ExpectedNoneGotNone,
            (None, Some(val)) => OptionDiff::ExpectedNoneGotSome(val),
            (Some(val), None) => OptionDiff::ExpectedSomeGotNone(val),
            (Some(a), Some(b)) => OptionDiff::ExpectedSomeGotSome(a.diff(b, time))
        }
    }
}

pub struct VecDiff<'exp, 'obs, ItemT: PartialInformationCompare<'exp, 'obs>> {
    missing: &'exp [ItemT],
    extra: &'obs [ItemT::Raw],
    common: Vec<ItemT::Diff>,
}

impl<'exp, 'obs, ItemT> PartialInformationCompare<'exp, 'obs> for Vec<ItemT>
    where ItemT: 'exp + PartialInformationCompare<'exp, 'obs>,
          ItemT::Raw: 'obs {
    type Raw = Vec<ItemT::Raw>;
    type Diff = VecDiff<'exp, 'obs, ItemT>;

    fn diff(&'exp self, other: &'obs Self::Raw, time: DateTime<Utc>) -> Self::Diff {
        VecDiff {
            missing: &self[other.len()..],
            extra: &other[self.len()..],
            common: iter::zip(self, other)
                .map(|(self_item, other_item)| self_item.diff(other_item, time))
                .collect(),
        }
    }
}


macro_rules! trivial_compare {
    ($($t:ty),+) => {
        $(impl<'exp, 'obs> PartialInformationCompare<'exp, 'obs> for $t {
            type Raw = Self;
            type Diff = Option<(&'exp $t, &'obs $t)>;

            fn diff(&'exp self, other: &'obs Self, _: DateTime<Utc>) -> Self::Diff {
                if self.eq(other) {
                    None
                } else {
                    Some((self, other))
                }
            }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);