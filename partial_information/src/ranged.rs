use std::fmt::Debug;
use std::ops;
use serde::{Deserialize, Deserializer};
use crate::PartialInformationFieldCompare;

#[derive(Clone)]
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

impl<UnderlyingType> ops::AddAssign<Ranged<UnderlyingType>> for Ranged<UnderlyingType>
    where UnderlyingType: ops::Add<UnderlyingType, Output=UnderlyingType> + Copy + Debug + PartialOrd {
    fn add_assign(&mut self, rhs: Ranged<UnderlyingType>) {
        *self = *self + rhs;
    }
}

impl<T> PartialInformationFieldCompare for Ranged<T>
    where T: Clone + Debug + PartialOrd {
    fn get_conflicts(field_path: String, expected: &Self, actual: &Self) -> Vec<String> {
        match actual {
            Ranged::Known(actual) => {
                match expected {
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