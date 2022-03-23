use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::iter;
use uuid::Uuid;
use std::iter::Iterator;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub trait PartialInformationDiff<'d>: Debug {
    fn is_empty(&self) -> bool;
}

#[derive(Debug)]
pub struct Conflict {
    property: String,
    message: String,
}

impl Conflict {
    pub fn new(property: String, message: String) -> Conflict {
        Conflict { property, message }
    }

    pub fn with_prefix(self, prefix: &str) -> Conflict {
        Conflict {
            property: format!("{}/{}", prefix, self.property),
            message: self.message,
        }
    }
}

impl Display for Conflict {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.property, self.message)
    }
}

pub trait PartialInformationCompare: Sized + Debug {
    type Raw: 'static + for<'de> Deserialize<'de> + Serialize + Debug + Send + Sync + Clone;
    type Diff<'d>: PartialInformationDiff<'d > where Self: 'd;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d>;
    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict>;

    fn from_raw(raw: Self::Raw) -> Self;
    fn raw_approximation(self) -> Self::Raw;
}


#[derive(Debug)]
pub struct HashMapDiff<'d, KeyT, ValT: PartialInformationCompare> {
    missing: HashMap<KeyT, &'d ValT>,
    extra: HashMap<KeyT, &'d ValT::Raw>,
    common: HashMap<KeyT, ValT::Diff<'d>>,
}

impl<K, V> PartialInformationCompare for HashMap<K, V>
    where K: 'static + Debug + Eq + Hash + Clone + for<'de> Deserialize<'de> + Serialize + Send + Sync,
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

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        let mut conflicts = Vec::new();

        for (key, val) in observed {
            match self.get(&key) {
                Some(_) => {}
                None => {
                    conflicts.push(
                        Conflict::new(format!("{:?}", key),
                                      format!("Expected no value in HashMap, but observed {:?}", val))
                    );
                }
            }
        }

        for (key, expected_val) in self.iter_mut() {
            match observed.get(&key) {
                None => {
                    conflicts.push(
                        Conflict::new(format!("{:?}", key),
                                      format!("Expected value {:?} in HashMap, but observed none", expected_val))
                    );
                }
                Some(observed_val) => {
                    conflicts.extend(
                        expected_val.observe(observed_val).into_iter()
                            .map(move |conflict| conflict.with_prefix(&format!("{:?}", key)))
                    )
                }
            }
        }

        conflicts
    }

    fn from_raw(raw: Self::Raw) -> Self {
        raw.into_iter()
            .map(|(key, raw_value)| (key, V::from_raw(raw_value)))
            .collect()
    }

    fn raw_approximation(self) -> Self::Raw {
        self.into_iter()
            .map(|(key, val)| (key, val.raw_approximation()))
            .collect()
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

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        match (self, observed) {
            (None, None) => vec![],
            (None, Some(val)) => {
                vec![Conflict::new(String::new(),
                                   format!("Expected no value in Option, but observed {:?}", val))]
            }
            (Some(val), None) => {
                vec![Conflict::new(String::new(),
                                   format!("Expected value {:?} in Option, but observed none", val))]
            }
            (Some(a), Some(b)) => a.observe(b)
        }
    }

    fn from_raw(raw: Self::Raw) -> Self {
        raw.map(|v| T::from_raw(v))
    }

    fn raw_approximation(self) -> Self::Raw {
        self.map(|val| val.raw_approximation())
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

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        let mut conflicts = Vec::new();

        if self.len() > observed.len() {
            conflicts.extend(
                self[observed.len()..].iter().enumerate()
                    .map(|(i, val)| Conflict::new(format!("{:?}", i),
                                                  format!("Expected value {:?} in Vec, but observed none", val)))
            );
        } else if observed.len() > self.len() {
            conflicts.extend(
                observed[self.len()..].iter().enumerate()
                    .map(|(i, val)| Conflict::new(format!("{:?}", i),
                                                  format!("Expected no value in Vec, but observed {:?}", val)))
            );
        }

        let rot_amt = conflicts.len();
        conflicts.extend(
            iter::zip(self, observed)
                .enumerate()
                .map(|(i, (self_item, other_item))| {
                    self_item.observe(other_item).into_iter()
                        .map(move |conflict| conflict.with_prefix(&format!("{:?}", i)))
                })
                .flatten()
        );

        // Rust lifetime rules force me to compute the extra elements first, but I want them to be
        // listed last. This rotation makes that happen.
        conflicts.rotate_left(rot_amt);

        conflicts
    }

    fn from_raw(raw: Self::Raw) -> Self {
        raw.into_iter()
            .map(|v| ItemT::from_raw(v))
            .collect()
    }

    fn raw_approximation(self) -> Self::Raw {
        self.into_iter()
            .map(|val| val.raw_approximation())
            .collect()
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

            fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
                if self == observed {
                    vec![]
                } else {
                    vec![Conflict::new(String::new(),
                                       format!("Expected {:?}, but observed {:?}", self, observed))]
                }
            }

            fn from_raw(raw: Self::Raw) -> Self { raw }
            fn raw_approximation(self) -> Self::Raw { self }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);