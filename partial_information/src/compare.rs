use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use uuid::Uuid;
use std::iter::Iterator;
use chrono::{DateTime, Utc};

pub trait PartialInformationCompare {
    fn get_conflicts(&self, other: &Self) -> Vec<String>;
}

pub trait PartialInformationFieldCompare {
    fn get_conflicts(field_path: String, expected: &Self, observed: &Self) -> Vec<String>;
}

impl<T> PartialInformationFieldCompare for T where T: PartialInformationCompare {
    fn get_conflicts(field_path: String, expected: &Self, observed: &Self) -> Vec<String> {
        expected.get_conflicts(observed).into_iter()
            .map(|s| format!("{}/{}", field_path, s))
            .collect()
    }
}

impl<K, V> PartialInformationFieldCompare for HashMap<K, V>
    where V: PartialInformationFieldCompare, K: Eq + Hash + Display {
    fn get_conflicts(field_path: String, expected: &Self, observed: &Self) -> Vec<String> {
        let expected_keys: HashSet<_> = expected.keys().collect();
        let observed_keys: HashSet<_> = expected.keys().collect();

        let iter1 = expected_keys.difference(&observed_keys)
            .map(|key| format!("{}/{} expected but was not observed", field_path, key));
        let iter2 = observed_keys.difference(&expected_keys)
            .map(|key| format!("{}/{} observed but was not expected", field_path, key));
        let iter3 = observed_keys.union(&expected_keys)
            .flat_map(|key| PartialInformationFieldCompare::get_conflicts(
                format!("{}/{}", field_path, key),
                expected.get(key).unwrap(),
                observed.get(key).unwrap()));

        iter1.chain(iter2).chain(iter3).collect()
    }
}

impl<ItemT> PartialInformationFieldCompare for Option<ItemT> where ItemT: PartialInformationFieldCompare + Debug {
    fn get_conflicts(field_path: String, expected: &Self, observed: &Self) -> Vec<String> {
        match (expected, observed) {
            (None, None) => vec![],
            (None, Some(val)) => vec![format!("{} Expected null, but observed {:?}", field_path, val)],
            (Some(val), None) => vec![format!("{} Expected {:?}, but observed null", field_path, val)],
            (Some(a), Some(b)) => PartialInformationFieldCompare::get_conflicts(field_path, a, b)
        }
    }
}

impl<ItemT> PartialInformationFieldCompare for Vec<ItemT> where ItemT: PartialInformationFieldCompare {
    fn get_conflicts(field_path: String, expected: &Self, observed: &Self) -> Vec<String> {
        if expected.len() != observed.len() {
            vec![format!("{}: Expected length was {}, but observed length was {}", field_path, expected.len(), observed.len())];
        }

        Iterator::zip(expected.iter(), observed.iter())
            .enumerate()
            .map(|(i, (a, b))| {
                PartialInformationFieldCompare::get_conflicts(format!("{}/{}", field_path, i), a, b)
            })
            .flatten()
            .collect()
    }
}


macro_rules! trivial_compare {
    ($($t:ty),+) => {
        $(impl PartialInformationFieldCompare for $t {
            fn get_conflicts(field_path: String, expected: &Self, observed: &Self) -> Vec<String> {
                if observed.eq(expected) {
                    return vec![];
                }

                vec![format!("{}: Expected {:?}, but observed {:?}", field_path, expected, observed)]
            }
        })+
    }
}

trivial_compare!(bool, f64, f32, i64, i32, i16, i8, isize, u64, u32, u16, u8, usize, Uuid, String, DateTime<Utc>);