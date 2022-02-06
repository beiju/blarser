use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Add;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Deserializer};
use crate::PartialInformationCompare;

#[derive(Clone, Debug)]
pub struct CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq,
          ValType: Clone + Debug + PartialOrd + PartialInformationCompare {
    pub values: HashMap<KeyType, ValType>,
    // Cached values also needs to cache the non-existence of a property, so it holds an option
    pub cached_values: HashMap<KeyType, (Option<ValType>, DateTime<Utc>)>,
}

impl<'de, KeyType, ValType> Deserialize<'de> for CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq + serde::Deserialize<'de>,
          ValType: for<'de2> Deserialize<'de2> + Clone + Debug + PartialOrd + PartialInformationCompare {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        Ok(CachedMap {
            values: Deserialize::deserialize(deserializer)?,
            cached_values: HashMap::new(),
        })
    }
}

impl<KeyType, ValType> PartialInformationCompare for CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq + Display,
          ValType: Clone + Debug + PartialOrd + PartialInformationCompare {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        let self_keys: HashSet<_> = self.values.keys().collect();
        let other_keys: HashSet<_> = other.values.keys().collect();

        let missing_keys = self_keys.difference(&other_keys)
            .filter_map(|&key| {
                if let Some((old_val, expiry)) = self.cached_values.get(key) {
                    if let Some(old_val) = old_val {
                        // Then there is an expected value and a cached value, but not an observed value
                        if expiry < &time {
                            Some(format!("- {}/{} expected but was not observed. Expected value: {:?} (note: there is an expired cached value {:?})",
                                         field_path, key, self.values.get(key).unwrap(), old_val))
                        } else {
                            Some(format!("- {}/{} expected but was not observed. Expected value: {:?} or cached value {:?}",
                                         field_path, key, self.values.get(key).unwrap(), old_val))
                        }
                    } else {
                        // Then we have cached the non-existence of this property
                        if expiry < &time {
                            Some(format!("- {}/{} expected but was not observed. Expected value: {:?} (note: there is an expired cached version where this property is not expected)",
                                         field_path, key, self.values.get(key).unwrap()))
                        } else {
                            None
                        }
                    }
                } else {
                    // Then there is no cached value
                    Some(format!("- {}/{} expected but was not observed. Expected value: {:?}",
                                 field_path, key, self.values.get(key).unwrap()))
                }
            });

        let invalid_values = other_keys.iter()
            .filter_map(|&key| Option::from({
                let observed_val = other.values.get(key)
                    .expect("Observed values must be known");

                match (self.values.get(key), self.cached_values.get(key)) {
                    (None, None) => {
                        Some(format!("- {}/{} observed but was not expected. Observed value: {:?}",
                                     field_path, key, observed_val))
                    }
                    (Some(expected_val), None) => {
                        expected_val.get_conflicts_internal(observed_val, time, &format!("{}/{}", field_path, key))
                    }
                    (None, Some((None, _))) => {
                        panic!("CachedMap had no primary entry for {} and a None entry in the cache, which should not be possible", key)
                    }
                    (None, Some((Some(cached_val), expiry))) => {
                        if let Some(cached_conflicts) = cached_val.get_conflicts_internal(observed_val, time, &format!("{}/{} [cached]", field_path, key)) {
                            if expiry < &time {
                                Some(format!("- {}/{} observed but was not expected. Observed value: {:?}. Note: There is an expired cached value, but it does not match:\n    {}",
                                             field_path, key, observed_val, cached_conflicts.lines().join("\n    ")))
                            } else {
                                Some(format!("- {}/{} observed but was not expected. Observed value: {:?}. There is a cached value, but it does not match:\n    {}",
                                             field_path, key, observed_val, cached_conflicts.lines().join("\n    ")))
                            }
                        } else {
                            if expiry < &time {
                                Some(format!("- {}/{} observed but was not expected. Observed value: {:?}. Note: There is an expired cached value which does match.",
                                             field_path, key, observed_val))
                            } else {
                                None
                            }
                        }
                    }
                    (Some(expected_val_current), Some((None, _))) => {
                        // We have a cached value but it's caching non-existence, and the property exists
                        // in the primary value and the observed value. Just ignore the cached value.
                        expected_val_current.get_conflicts_internal(observed_val, time, &format!("{}/{}", field_path, key))
                    }
                    (Some(expected_val_current), Some((Some(expected_val_next), expiry))) => {
                        let conflicts_current = expected_val_current.get_conflicts_internal(
                            observed_val, time, &format!("{}/{}", field_path, key));
                        if let Some(conflicts_current) = conflicts_current {
                            let conflicts_cached = expected_val_next.get_conflicts_internal(
                                observed_val, time, &format!("{}/{}", field_path, key));
                            if let Some(conflicts_cached) = conflicts_cached {
                                if expiry < &time {
                                    Some(format!("- {}/{}: Doesn't match:\n    {}Note: There is an expired cached value, but it also doesn't match:\n    {}",
                                                 field_path, key,
                                                 conflicts_current.lines().join("\n    "),
                                                 conflicts_cached.lines().join("\n    ")))
                                } else {
                                    Some(format!("- {}/{}: Doesn't match:\n    {}And doesn't match cached value:\n    {}",
                                                 field_path, key,
                                                 conflicts_current.lines().join("\n    "),
                                                 conflicts_cached.lines().join("\n    ")))
                                }
                            } else {
                                if expiry < &time {
                                    Some(format!("- {}/{}: Doesn't match:\n    {}Note: There is an expired cached value which does match",
                                                 field_path, key,
                                                 conflicts_current.lines().join("\n    ")))
                                } else {
                                    // Non-expired cached version matched
                                    None
                                }
                            }
                        } else {
                            // Current version matched
                            None
                        }
                    }
                }
            }));

        let output = missing_keys.chain(invalid_values).join("\n");
        if output.is_empty() { None } else { Some(output) }
    }
}

impl<KeyType, ValType> CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq + Display + Clone,
          ValType: Clone + Debug + PartialOrd + PartialInformationCompare {
    pub fn insert(&mut self, key: KeyType, value: ValType, expiry: DateTime<Utc>) {
        let old_val = self.values.insert(key.clone(), value);
        // Insert old_val always, because we also want to cache non-existence
        self.cached_values.insert(key, (old_val, expiry));
    }

    pub fn add_with_default<AddType>(&mut self, key: KeyType, to_add: AddType, expiry: DateTime<Utc>)
        where ValType: Default,
              for<'a> &'a ValType: Add<AddType, Output=ValType> {
        let new_val = self.values.get(&key).unwrap_or(&Default::default()) + to_add;
        self.insert(key, new_val, expiry)
    }
}