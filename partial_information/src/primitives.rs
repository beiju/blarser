use std::fmt::Debug;
use serde::{Deserialize, Deserializer};
use crate::PartialInformationFieldCompare;

pub enum Ranged<UnderlyingType: PartialOrd + Debug> {
    Unknown,
    Known(UnderlyingType),
    Range(UnderlyingType, UnderlyingType),
}

impl<'de, UnderlyingType> Deserialize<'de> for Ranged<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + PartialOrd + Debug {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(Ranged::Known(val))
    }
}

impl<T: PartialOrd + Debug> PartialInformationFieldCompare for Ranged<T> {
    fn get_conflicts(field_path: String, expected: &Self, actual: &Self) -> Vec<String> {
        match actual {
            Ranged::Known(actual) => {
                match expected {
                    Ranged::Unknown => { vec![] }
                    Ranged::Known(expected) => {
                        if actual == expected {
                            vec![]
                        } else {
                            vec![format!("{}: Expected {:?}, but value was {:?}", field_path, expected, actual)]
                        }
                    }
                    Ranged::Range(lower, upper) => {
                        if lower < actual && actual < upper {
                            vec![]
                        } else {
                            vec![format!("{}: Expected value between {:?} and {:?}, but value was {:?}", field_path, lower, upper, actual)]
                        }
                    }
                }
            }
            _ => {
                panic!("Actual value must be Known")
            }
        }
    }
}