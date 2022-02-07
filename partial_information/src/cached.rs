use std::fmt::Debug;
use std::ops::Add;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Deserializer};

use crate::PartialInformationCompare;

#[derive(Debug, Clone)]
pub struct Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    value: UnderlyingType,
    cached: Option<(UnderlyingType, DateTime<Utc>)>,
}

impl<'de, UnderlyingType> Deserialize<'de> for Cached<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        Ok(Cached {
            value: Deserialize::deserialize(deserializer)?,
            cached: None,
        })
    }
}

impl<T> PartialInformationCompare for Cached<T>
    where T: Clone + Debug + PartialInformationCompare {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
        let (primary_val_conflicts, primary_val_canonical) = self.value.get_conflicts_internal(&other.value, time, field_path);
        if let Some(primary_val_conflicts) = primary_val_conflicts {
            if let Some((cached_val, expiry)) = &self.cached {
                // Cached value is always non-canonical, no matter what its descendants say
                let (cached_val_conflicts, _) = cached_val.get_conflicts_internal(&other.value, time, field_path);
                if let Some(cached_val_conflicts) = cached_val_conflicts {
                    // Neither primary nor cached match
                    let expired_txt = if expiry < &time {
                        "expired "
                    } else {
                        ""
                    };

                    (Some(format!("- {}: Primary value doesn't match:\n    {}\nand {}cached value doesn't match:\n    {}",
                                  field_path, expired_txt,
                                  primary_val_conflicts.lines().join("\n    "),
                                  cached_val_conflicts.lines().join("\n    "))), true)
                } else if expiry < &time {
                    // Primary doesn't match; cached does but is expired
                    (Some(format!("- {}: Cached value matches but is expired. Primary value doesn't match:\n    {}",
                                  field_path, primary_val_conflicts.lines().join("\n    "))), true)
                } else {
                    // Primary doesn't match but cached does and it's not expired
                    (None, false) // non-canonical!
                }
            } else {
                // Primary doesn't match and there's no cached value
                (Some(primary_val_conflicts + " [no cached value]"), true)
            }
        } else {
            // This update isn't canonical if there's a non-expired cached value, even if the
            // primary value matches. Canonical updates must be reconstructible from the API data
            // alone, and the API can't tell us the cached value.
            let mut canonical = primary_val_canonical;
            if let Some((_, expiry)) = self.cached {
                if !(expiry < time) {
                    canonical = false;
                }
            }
            (None, canonical)
        }
    }
}

impl<UnderlyingType> Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug {
    pub fn set_uncached(&mut self, value: UnderlyingType) {
        self.value = value;
        self.cached = None;
    }

    pub fn update_uncached<F>(&mut self, update_fn: F)
        where F: FnOnce(&UnderlyingType) -> UnderlyingType {
        self.set_uncached(update_fn(&self.value));
    }

    pub fn set_cached(&mut self, value: UnderlyingType, deadline: DateTime<Utc>) {
        let old_val = std::mem::replace(&mut self.value, value);
        println!("Caching {:?}", old_val);
        // It's deadline from the perspective of the new value and expiry from the
        // perspective of the old value
        self.cached = Some((old_val, deadline));
    }

    pub fn add_cached<AddT>(&mut self, to_add: AddT, deadline: DateTime<Utc>)
        where for<'a> &'a UnderlyingType: Add<AddT, Output=UnderlyingType> {
        let new_val = &self.value + to_add;
        self.set_cached(new_val, deadline);
    }
}