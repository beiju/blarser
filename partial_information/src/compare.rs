use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use uuid::Uuid;
use std::iter::Iterator;
use itertools::Itertools;
use chrono::{DateTime, Utc};

pub trait PartialInformationCompare
    where Self::Raw: Debug + for<'de> ::serde::Deserialize<'de> {
    type Raw;

    fn get_conflicts(&self, other: &Self::Raw, time: DateTime<Utc>) -> (Option<String>, bool) {
        self.get_conflicts_internal(other, time, &String::new())
    }

    fn get_conflicts_internal(&self, other: &Self::Raw, time: DateTime<Utc>, field_path: &str) -> (Option<String>, bool);
}

impl<K, V> PartialInformationCompare for HashMap<K, V>
    where V: PartialInformationCompare,
          K: Eq + Hash + Display + Debug + for<'de> ::serde::Deserialize<'de>,
          V::Raw: for<'de> ::serde::Deserialize<'de> {
    type Raw = HashMap<K, V::Raw>;

    fn get_conflicts_internal(&self, observed: &Self::Raw, time: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
        let expected_keys: HashSet<_> = self.keys().collect();
        let observed_keys: HashSet<_> = observed.keys().collect();

        let iter1 = expected_keys.difference(&observed_keys)
            .map(|key| format!("- {}/{} expected but was not observed", field_path, key));
        let iter2 = observed_keys.difference(&expected_keys)
            .map(|key| format!("- {}/{} observed but was not expected", field_path, key));
        let all_canonical = &mut true;
        let iter3 = observed_keys.intersection(&expected_keys)
            .filter_map(|key| {
                let (conflicts, canonical) = self.get(key).unwrap().get_conflicts_internal(
                    observed.get(key).unwrap(),
                    time, &format!("{}/{}", field_path, key));
                *all_canonical &= canonical;
                conflicts
            });


        let output = iter1.chain(iter2).chain(iter3).join("\n");
        if !*all_canonical { println!("Vec not canonical") };
        if output.is_empty() {
            (None, *all_canonical)
        } else {
            (Some(output), *all_canonical)
        }
    }
}

impl<ItemT> PartialInformationCompare for Option<ItemT>
    where ItemT: PartialInformationCompare + Debug,
          ItemT::Raw: Debug {
    type Raw = Option<ItemT::Raw>;

    fn get_conflicts_internal(&self, other: &Self::Raw, time: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
        match (self, other) {
            (None, None) => (None, true),
            (None, Some(val)) => (Some(format!("- {} Expected null, but observed {:?}", field_path, val)), true),
            (Some(val), None) => (Some(format!("- {} Expected {:?}, but observed null", field_path, val)), true),
            (Some(a), Some(b)) => a.get_conflicts_internal(b, time, field_path)
        }
    }
}

impl<ItemT> PartialInformationCompare for Vec<ItemT> where ItemT: PartialInformationCompare {
    type Raw = Vec<ItemT::Raw>;

    fn get_conflicts_internal(&self, other: &Self::Raw, time: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
        if self.len() != other.len() {
            return (Some(format!("- {}: Expected length was {}, but observed length was {}", field_path, self.len(), other.len())), true);
        }

        let mut all_canonical = true;
        let all_canonical_ref = &mut all_canonical;
        let output = Iterator::zip(self.iter(), other.iter())
            .enumerate()
            .filter_map(|(i, (a, b))| {
                let (conflicts, canonical) = a.get_conflicts_internal(b, time, &format!("{}/{}", field_path, i));
                *all_canonical_ref &= canonical;
                conflicts
            })
            .join("\n");
        if output.is_empty() {
            (None, all_canonical)
        } else {
            (Some(output), all_canonical)
        }
    }
}


macro_rules! trivial_compare {
    ($($t:ty),+) => {
        $(impl PartialInformationCompare for $t {
            type Raw = Self;
            fn get_conflicts_internal(&self, other: &Self, _: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
                if self.eq(other) {
                    (None, true)
                } else {
                    (Some(format!("- {}: Expected {:?}, but observed {:?}", field_path, self, other)), true)
                }
            }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);