use std::fmt::Debug;
use serde::{Deserialize, Deserializer};
use crate::PartialInformationFieldCompare;

#[derive(Clone)]
pub enum MaybeKnown<UnderlyingType: Clone + Debug + PartialOrd > {
    Unknown,
    Known(UnderlyingType),
}

impl<UnderlyingType> MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug + PartialOrd {
    pub fn known(&self) -> Option<&UnderlyingType> {
        match self {
            MaybeKnown::Unknown => { None }
            MaybeKnown::Known(val) => { Some(val) }
        }
    }
}

impl<'de, UnderlyingType> Deserialize<'de> for MaybeKnown<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug + PartialOrd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(MaybeKnown::Known(val))
    }
}

impl<UnderlyingType> From<UnderlyingType> for MaybeKnown<UnderlyingType>
    where UnderlyingType: Clone + Debug + PartialOrd {
    fn from(item: UnderlyingType) -> Self {
        Self::Known(item)
    }
}

impl<T> PartialInformationFieldCompare for MaybeKnown<T>
    where T: Clone + Debug + PartialOrd {
    fn get_conflicts(field_path: String, expected: &Self, actual: &Self) -> Vec<String> {
        match actual {
            MaybeKnown::Known(actual) => {
                match expected {
                    MaybeKnown::Unknown => { vec![] }
                    MaybeKnown::Known(expected) => {
                        if actual == expected {
                            vec![]
                        } else {
                            vec![format!("{}: Expected {:?}, but value was {:?}", field_path, expected, actual)]
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