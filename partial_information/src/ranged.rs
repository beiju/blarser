use std::fmt::Debug;
use std::ops;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer};
use crate::PartialInformationCompare;

#[derive(Debug, Clone)]
pub enum Ranged<UnderlyingType: Clone + Debug + PartialOrd> {
    Known(UnderlyingType),
    Range(UnderlyingType, UnderlyingType),
}

// Ranged is Copy if underlying type is Copy
impl<UnderlyingType> Copy for Ranged<UnderlyingType>
    where UnderlyingType: Copy + Clone + Debug + PartialOrd {}

impl<UnderlyingType> Ranged<UnderlyingType>
    where UnderlyingType: Ord + Clone + Debug + PartialOrd {
    pub fn could_be(&self, other: &UnderlyingType) -> bool {
        match self {
            Ranged::Known(a) => { a == other }
            Ranged::Range(lower, upper) => {
                lower <= other && other <= upper
            }
        }
    }
}

impl<'de, UnderlyingType> Deserialize<'de> for Ranged<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> + Clone + Debug + PartialOrd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(Ranged::Known(val))
    }
}

// This requires Copy for simplicity of implementation, you could make it work without Copy
impl<UnderlyingType> ops::Add<Ranged<UnderlyingType>> for Ranged<UnderlyingType>
    where UnderlyingType: ops::Add<UnderlyingType, Output=UnderlyingType> + Copy + Debug + PartialOrd {
    type Output = Ranged<UnderlyingType>;

    fn add(self, rhs: Ranged<UnderlyingType>) -> Ranged<UnderlyingType> {
        match (self, rhs) {
            (Ranged::Known(a), Ranged::Known(b)) => {
                Ranged::Known(a + b)
            }
            (Ranged::Known(a), Ranged::Range(b1, b2)) => {
                Ranged::Range(a + b1, a + b2)
            }
            (Ranged::Range(a1, a2), Ranged::Known(b)) => {
                Ranged::Range(a1 + b, a2 + b)
            }
            (Ranged::Range(a1, a2), Ranged::Range(b1, b2)) => {
                Ranged::Range(a1 + b1, a2 + b2)
            }
        }
    }
}

impl<'a, UnderlyingType> ops::Add<Ranged<UnderlyingType>> for &'a Ranged<UnderlyingType>
    where UnderlyingType: Copy + Debug + PartialOrd,
          &'a UnderlyingType: ops::Add<UnderlyingType, Output=UnderlyingType> {
    type Output = Ranged<UnderlyingType>;

    //noinspection DuplicatedCode
    fn add(self, rhs: Ranged<UnderlyingType>) -> Ranged<UnderlyingType> {
        match (self, rhs) {
            (Ranged::Known(a), Ranged::Known(b)) => {
                Ranged::Known(a + b)
            }
            (Ranged::Known(a), Ranged::Range(b1, b2)) => {
                Ranged::Range(a + b1, a + b2)
            }
            (Ranged::Range(a1, a2), Ranged::Known(b)) => {
                Ranged::Range(a1 + b, a2 + b)
            }
            (Ranged::Range(a1, a2), Ranged::Range(b1, b2)) => {
                Ranged::Range(a1 + b1, a2 + b2)
            }
        }
    }
}

impl<UnderlyingType> ops::AddAssign<Ranged<UnderlyingType>> for Ranged<UnderlyingType>
    where UnderlyingType: ops::Add<UnderlyingType, Output=UnderlyingType> + Copy + Debug + PartialOrd {
    fn add_assign(&mut self, rhs: Ranged<UnderlyingType>) {
        *self = *self + rhs;
    }
}


// This requires Copy for simplicity of implementation, you could make it work without Copy
impl<UnderlyingType> ops::Add<UnderlyingType> for Ranged<UnderlyingType>
    where UnderlyingType: ops::Add<UnderlyingType, Output=UnderlyingType> + Copy + Debug + PartialOrd {
    type Output = Ranged<UnderlyingType>;

    fn add(self, rhs: UnderlyingType) -> Ranged<UnderlyingType> {
        match self {
            Ranged::Known(val) => { Ranged::Known(val + rhs) }
            Ranged::Range(a, b) => {
                Ranged::Range(a + rhs, b + rhs)
            }
        }
    }
}

impl<UnderlyingType> ops::AddAssign<UnderlyingType> for Ranged<UnderlyingType>
    where UnderlyingType: ops::AddAssign<UnderlyingType> + Copy + Debug + PartialOrd {
    fn add_assign(&mut self, rhs: UnderlyingType) {
        match self {
            Ranged::Known(val) => {
                *val += rhs;
            }
            Ranged::Range(a, b) => {
                *a += rhs;
                *b += rhs;
            }
        }
    }
}

impl<T> PartialInformationCompare for Ranged<T>
    where T: Clone + Debug + PartialOrd {
    fn get_conflicts_internal(&self, other: &Self, _: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
        (match other {
            Ranged::Known(actual) => {
                match self {
                    Ranged::Known(expected) => {
                        if actual == expected {
                            None
                        } else {
                            Some(format!("- {}: Expected {:?}, but value was {:?}",
                                         field_path, expected, actual))
                        }
                    }
                    Ranged::Range(lower, upper) => {
                        if lower <= actual && actual <= upper {
                            None
                        } else {
                            Some(format!("- {}: Expected value between {:?} and {:?}, but value was {:?}",
                                         field_path, lower, upper, actual))
                        }
                    }
                }
            }
            _ => {
                panic!("Actual value must be Known")
            }
        }, true) // Ranged is always canonical
    }
}