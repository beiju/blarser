use std::collections::{HashMap};
use std::hash::Hash;
use std::ops::Add;
use chrono::{DateTime, Utc};
use crate::PartialInformationCompare;

pub struct CachedMap<K, V>
    where K: Hash + Eq + Clone,
          V: for<'exp, 'obs> PartialInformationCompare<'exp, 'obs> {
    pub values: HashMap<K, V>,
    // Cached values also needs to cache the non-existence of a property, so it holds an option
    pub cached_values: HashMap<K, (Option<V>, DateTime<Utc>)>,
}

// TODO This will be easier to write once I start writing the methods on Diff types
pub struct CachedMapDiff {}

impl<'exp, 'obs, K, V> PartialInformationCompare<'exp, 'obs> for CachedMap<K, V>
    where K: 'exp + 'obs + Hash + Eq + Clone,
          V: 'exp + for<'e, 'o> PartialInformationCompare<'e, 'o>,
          <V as PartialInformationCompare<'exp, 'obs>>::Raw: 'obs {
    type Raw = HashMap<K, <V as PartialInformationCompare<'exp, 'obs>>::Raw>;
    type Diff = CachedMapDiff;

    fn diff(&'exp self, _other: &'obs Self::Raw, _time: DateTime<Utc>) -> Self::Diff {
        CachedMapDiff {}
    }
}

impl<KeyType, ValType> CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq + Clone,
          ValType: for<'exp, 'obs> PartialInformationCompare<'exp, 'obs> {
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