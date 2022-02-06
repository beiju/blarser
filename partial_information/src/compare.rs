use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use uuid::Uuid;
use std::iter::Iterator;
use itertools::Itertools;
use chrono::{DateTime, Utc};

pub trait PartialInformationCompare {
    fn get_conflicts(&self, other: &Self, time: DateTime<Utc>) -> Option<String> {
        self.get_conflicts_internal(other, time, &String::new())
    }

    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String>;
}

impl<K, V> PartialInformationCompare for HashMap<K, V>
    where V: PartialInformationCompare, K: Eq + Hash + Display {
    fn get_conflicts_internal(&self, observed: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        let expected_keys: HashSet<_> = self.keys().collect();
        let observed_keys: HashSet<_> = observed.keys().collect();

        let iter1 = expected_keys.difference(&observed_keys)
            .map(|key| format!("- {}/{} expected but was not observed", field_path, key));
        let iter2 = observed_keys.difference(&expected_keys)
            .map(|key| format!("- {}/{} observed but was not expected", field_path, key));
        let iter3 = observed_keys.intersection(&expected_keys)
            .filter_map(|key| self.get(key).unwrap().get_conflicts_internal(
                observed.get(key).unwrap(),
                time, &format!("{}/{}", field_path, key)));

        let output = iter1.chain(iter2).chain(iter3).join("\n");
        if output.is_empty() {
            None
        } else {
            Some(output)
        }
    }
}

impl<ItemT> PartialInformationCompare for Option<ItemT> where ItemT: PartialInformationCompare + Debug {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        match (self, other) {
            (None, None) => None,
            (None, Some(val)) => Some(format!("- {} Expected null, but observed {:?}", field_path, val)),
            (Some(val), None) => Some(format!("- {} Expected {:?}, but observed null", field_path, val)),
            (Some(a), Some(b)) => a.get_conflicts_internal(b, time, field_path)
        }
    }
}

impl<ItemT> PartialInformationCompare for Vec<ItemT> where ItemT: PartialInformationCompare {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        if self.len() != other.len() {
            return Some(format!("- {}: Expected length was {}, but observed length was {}", field_path, self.len(), other.len()));
        }

        let output = Iterator::zip(self.iter(), other.iter())
            .enumerate()
            .filter_map(|(i, (a, b))| {
                a.get_conflicts_internal(b, time, &format!("{}/{}", field_path, i))
            })
            .join("\n");
        if output.is_empty() {
            None
        } else {
            Some(output)
        }
    }
}


macro_rules! trivial_compare {
    ($($t:ty),+) => {
        $(impl PartialInformationCompare for $t {
            fn get_conflicts_internal(&self, other: &Self, _: DateTime<Utc>, field_path: &str) -> Option<String> {
                if self.eq(other) {
                    None
                } else {
                    Some(format!("- {}: Expected {:?}, but observed {:?}", field_path, self, other))
                }
            }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);