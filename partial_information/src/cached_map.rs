use std::collections::{HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Add;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use crate::compare::PartialInformationDiff;
use crate::PartialInformationCompare;

#[derive(Clone, Debug, Deserialize)]
pub struct CachedMap<K, V>
    where K: Hash + Eq + Clone + Debug,
          V: PartialInformationCompare {
    pub values: HashMap<K, V>,
    // Cached values also needs to cache the non-existence of a property, so it holds an option
    pub cached_values: HashMap<K, (Option<V>, DateTime<Utc>)>,
}

impl<KeyType, ValType> CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq + Clone + Debug,
          ValType: PartialInformationCompare {
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

// TODO This will be easier to write once I start writing the methods on Diff types
#[derive(Debug)]
pub struct CachedMapDiff<'d, K, V: PartialInformationCompare> {
    dummy1: HashMap<K, &'d V>,
    dummy2: HashMap<K, &'d V::Raw>,
}

impl<K, V> PartialInformationCompare for CachedMap<K, V>
    where K: 'static + Hash + Eq + Clone + for<'de> Deserialize<'de> + Debug,
          V: 'static + PartialInformationCompare {
    type Raw = HashMap<K, V::Raw>;
    type Diff<'d> = CachedMapDiff<'d, K, V>;

    fn diff(&self, _observed: &HashMap<K, V::Raw>, _: DateTime<Utc>) -> Self::Diff<'_> {
        todo!()
    }
}

impl<'d, K, V> PartialInformationDiff<'d> for CachedMapDiff<'d, K, V>
    where K: 'd + Hash + Eq + Clone + Debug,
          V: 'd + PartialInformationCompare {
    fn is_empty(&self) -> bool {
        todo!()
    }
}