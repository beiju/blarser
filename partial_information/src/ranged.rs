use std::fmt::Debug;
use std::ops;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::PartialInformationCompare;

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub enum Ranged<UnderlyingType: PartialOrd> {
    Known(UnderlyingType),
    Range(UnderlyingType, UnderlyingType),
}

// Ranged is Clone if underlying type is Clone
impl<UnderlyingType> Clone for Ranged<UnderlyingType>
    where UnderlyingType: Clone + PartialOrd {
    fn clone(&self) -> Self {
        match self {
            Ranged::Known(x) => { Ranged::Known(x.clone()) }
            Ranged::Range(a, b) => { Ranged::Range(a.clone(), b.clone()) }
        }
    }
}

// Ranged is Copy if underlying type is Copy
impl<UnderlyingType> Copy for Ranged<UnderlyingType>
    where UnderlyingType: Copy + Clone + PartialOrd {}

impl<UnderlyingType> Ranged<UnderlyingType>
    where UnderlyingType: PartialOrd + Clone + PartialOrd {
    pub fn could_be(&self, other: &UnderlyingType) -> bool {
        match self {
            Ranged::Known(a) => { a == other }
            Ranged::Range(lower, upper) => {
                lower <= other && other <= upper
            }
        }
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

#[derive(Debug)]
pub enum RangedDiff<'d, T> {
    NoDiff,
    KnownDiff(&'d T, &'d T),
    RangeDiff((&'d T, &'d T), &'d T)
}

impl<T> PartialInformationCompare for Ranged<T>
    where T: 'static + Clone + Debug + PartialOrd + for<'de> Deserialize<'de> + Serialize + Send {
    type Raw = T;
    type Diff<'d> = RangedDiff<'d, T>;

    fn diff<'d>(&'d self, observed: &'d Self::Raw, _: DateTime<Utc>) -> Self::Diff<'d> {
        match self {
            Ranged::Known(value) => {
                if value == observed {
                    RangedDiff::NoDiff
                } else {
                    RangedDiff::KnownDiff(value, observed)
                }
            }
            Ranged::Range(low, high) => {
                if low <= observed && observed <= high {
                    RangedDiff::NoDiff
                } else {
                    RangedDiff::RangeDiff((low, high), observed)
                }
            }
        }
    }

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        match self {
            Ranged::Known(known) => {
                if known == observed {
                    vec![]
                } else {
                    vec![Conflict::new(String::new(),
                                       format!("Expected {:?}, but observed {:?}", known, observed))]
                }
            }
            Ranged::Range(lower, upper) => {
                if *lower <= *observed && observed <= upper {
                    *self = Ranged::Known((*observed).clone());
                    vec![]
                } else {
                    vec![Conflict::new(String::new(),
                                       format!("Expected value in range {:?}-{:?}, but observed {:?}", lower, upper, observed))]
                }
            }
        }
    }

    fn from_raw(raw: Self::Raw) -> Self {
        Ranged::Known(raw)
    }
}

impl<'d, T> PartialInformationDiff<'d> for RangedDiff<'d, T>
    where T: PartialOrd + Debug {
    fn is_empty(&self) -> bool {
        match self {
            RangedDiff::NoDiff => { true }
            RangedDiff::KnownDiff(_, _) => { false }
            RangedDiff::RangeDiff(_, _) => { false }
        }
    }
}