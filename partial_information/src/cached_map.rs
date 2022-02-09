use std::collections::{HashMap};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Add;
use chrono::{DateTime, Utc};
use crate::compare::PartialInformationDiff;
use crate::PartialInformationCompare;

pub struct CachedMap<K, V>
    where K: Hash + Eq + Clone,
          V: PartialInformationCompare {
    pub values: HashMap<K, V>,
    // Cached values also needs to cache the non-existence of a property, so it holds an option
    pub cached_values: HashMap<K, (Option<V>, DateTime<Utc>)>,
}

impl<KeyType, ValType> CachedMap<KeyType, ValType>
    where KeyType: Hash + Eq + Clone,
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
pub struct CachedMapDiff<'exp, 'obs, K, V: PartialInformationCompare> {
    dummy1: HashMap<K, &'exp V>,
    dummy2: HashMap<K, &'obs V::Raw>,
}

impl<K, V> PartialInformationCompare for CachedMap<K, V>
    where K: Hash + Eq + Clone,
          V: PartialInformationCompare {
    type Raw = HashMap<K, V::Raw>;
    type Diff<'exp, 'obs> = CachedMapDiff<'exp, 'obs, K, V>;

    fn diff<'exp, 'obs>(&'exp self, _observed: &'obs HashMap<K, V>, _: DateTime<Utc>) -> Self::Diff<'exp, 'obs> {
        todo!()
    }
}

impl<'exp, 'obs, K, V> PartialInformationDiff<'exp, 'obs> for CachedMapDiff<'exp, 'obs, K, V>
    where K: 'exp + Hash + Eq + Clone,
          V: 'exp + PartialInformationCompare {
    fn is_empty(&self) -> bool {
        todo!()
    }
}