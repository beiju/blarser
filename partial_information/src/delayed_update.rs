use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Add;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Deserializer};
use crate::PartialInformationCompare;

#[derive(Clone, Debug)]
pub struct DelayedUpdateMap<KeyType, ValType>
    where KeyType: Hash + Eq,
          ValType: Clone + Debug + PartialOrd + PartialInformationCompare {
    pub values: HashMap<KeyType, ValType>,
    pub next_values: HashMap<KeyType, (ValType, DateTime<Utc>)>,
}

impl<'de, KeyType, ValType> Deserialize<'de> for DelayedUpdateMap<KeyType, ValType>
    where KeyType: Hash + Eq + serde::Deserialize<'de>,
          ValType: for<'de2> Deserialize<'de2> + Clone + Debug + PartialOrd + PartialInformationCompare {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        Ok(DelayedUpdateMap {
            values: Deserialize::deserialize(deserializer)?,
            next_values: HashMap::new(),
        })
    }
}

impl<KeyType, ValType> PartialInformationCompare for DelayedUpdateMap<KeyType, ValType>
    where KeyType: Hash + Eq + Display,
          ValType: Clone + Debug + PartialOrd + PartialInformationCompare {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        let expected_keys: HashSet<_> = self.values.keys().collect();
        let observed_keys: HashSet<_> = other.values.keys().collect();

        let iter1 = expected_keys.difference(&observed_keys)
            .map(|key| format!("{}/{} expected but was not observed. Expected value: {:?}",
                               field_path, key, other.values.get(key).unwrap()));
        let iter2 = observed_keys.iter()
            .filter_map(|&key| Option::from({
                let observed_val = other.values.get(key)
                    .expect("Observed values must be known");

                match (self.values.get(key), self.next_values.get(key)) {
                    (None, None) => {
                        Some(format!("{}/{} observed but was not expected. Observed value: {:?}",
                                     field_path, key, observed_val))
                    }
                    (Some(expected_val), None) => {
                        expected_val.get_conflicts_internal(observed_val, time, &format!("{}/{}", field_path, key))
                    }
                    (None, Some((expected_val, _))) => {
                        expected_val.get_conflicts_internal(observed_val, time, &format!("{}/{} [next]", field_path, key))
                    }
                    (Some(expected_val_current), Some((expected_val_next, deadline))) => {
                        let conflicts_current = expected_val_current.get_conflicts_internal(
                            observed_val, time, &format!("{}/{}", field_path, key));
                        let conflicts_next = expected_val_next.get_conflicts_internal(
                            observed_val, time, &format!("{}/{}", field_path, key));

                        if conflicts_next.is_none() {
                            None
                        } else if time > *deadline && conflicts_current.is_none() {
                            // Abusing join() to indent a multiline string  (except the first line, which I do manually)
                            Some(format!("- {}/{}: Matches old value, but old value is expired. Doesn't match new value:\n    {}",
                                         field_path, key, conflicts_next.unwrap().lines().join("\n    ")))
                        } else {
                            Some(format!("- {}/{}: Doesn't match old value:\n    {}and doesn't match new value:\n    {}",
                                         field_path, key,
                                         conflicts_current.unwrap().lines().join("\n    "),
                                         conflicts_next.unwrap().lines().join("\n    ")))
                        }
                    }
                }
            }));

        let output = iter1.chain(iter2).join("\n");
        if output.is_empty() { None } else { Some(output) }
    }
}


impl<KeyType, ValType> DelayedUpdateMap<KeyType, ValType>
    where KeyType: Hash + Eq + Display,
          ValType: Add<Output=ValType> + Default + Clone + Debug + PartialOrd + PartialInformationCompare {
    pub fn add_with_default(&mut self, key: KeyType, to_add: ValType, deadline: DateTime<Utc>) {
        assert!(!self.next_values.contains_key(&key),
                "Can't add a value when one is already queued");

        let current_val = self.values.get(&key).cloned().unwrap_or(Default::default());
        self.next_values.insert(key, (current_val + to_add, deadline));
    }
}