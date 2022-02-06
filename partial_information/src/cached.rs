use std::fmt::Debug;
use std::ops::Add;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::{Deserialize, Deserializer};

use crate::PartialInformationCompare;

// A wrapper for properties that sometimes return spurious values, like Team.win_streak. For now,
// assumes the spurious value is Default::default(). This is hard to change without generic-type
// const generics.
#[derive(Debug, Clone)]
pub struct Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug + Default + PartialOrd {
    value: UnderlyingType,
    cached: Option<(UnderlyingType, DateTime<Utc>)>,
}

impl<'de, UnderlyingType> Deserialize<'de> for Cached<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug + Default + PartialOrd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        Ok(Cached {
            value: Deserialize::deserialize(deserializer)?,
            cached: None,
        })
    }
}

impl<T> PartialInformationCompare for Cached<T>
    where T: Clone + Debug + Default + PartialOrd + PartialInformationCompare {
    fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> Option<String> {
        let primary_val_conflicts = self.value.get_conflicts_internal(&other.value, time, field_path);
        if let Some(primary_val_conflicts) = primary_val_conflicts {
            if let Some((cached_val, expiry)) = &self.cached {
                let cached_val_conflicts = cached_val.get_conflicts_internal(&other.value, time, field_path);
                if let Some(cached_val_conflicts) = cached_val_conflicts {
                    // Neither primary nor cached match
                    let expired_txt = if expiry < &time {
                        "expired "
                    } else {
                        ""
                    };

                    Some(format!("- {}: Primary value doesn't match:\n    {}\nand {}cached value doesn't match:\n    {}",
                                 field_path, expired_txt,
                                 primary_val_conflicts.lines().join("\n    "),
                                 cached_val_conflicts.lines().join("\n    ")))
                } else if expiry < &time {
                    // Primary doesn't match; cached does but is expired
                    Some(format!("- {}: Cached value matches but is expired. Primary value doesn't match:\n    {}",
                                 field_path, primary_val_conflicts.lines().join("\n    ")))
                } else {
                    // Primary doesn't match but cached does and it's not expired
                    None
                }
            } else {
                // Primary doesn't match and there's no cached value
                Some(primary_val_conflicts + " [no cached value]")
            }
        } else {
            None
        }
    }
}

impl<UnderlyingType> Cached<UnderlyingType>
    where UnderlyingType: Clone + Debug + Default + PartialOrd {
    pub fn add<AddT>(&mut self, to_add: AddT, expiry: DateTime<Utc>)
        where for<'a> &'a UnderlyingType: Add<AddT, Output=UnderlyingType> {
        let new_val = &self.value + to_add;
        let old_val = std::mem::replace(&mut self.value, new_val);
        println!("Caching {:?}", old_val);
        self.cached = Some((old_val, expiry));
    }
}