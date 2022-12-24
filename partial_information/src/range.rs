use std::fmt::Debug;
use std::ops::{Add, AddAssign};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::{MaybeKnown, PartialInformationCompare};

#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RangeInclusive<UnderlyingType> {
    pub lower: UnderlyingType,
    pub upper: UnderlyingType,
}

impl<T> RangeInclusive<T> where T: for<'a> AddAssign<&'a T> {
    pub fn could_be(&self, observed: &T) -> bool where T: PartialOrd {
        !(observed < &self.lower || observed > &self.upper)
    }

    pub fn update(&mut self, raw: T) where T: Clone {
        self.lower = raw.clone();
        self.upper = raw;
    }

    pub fn add_constant(&mut self, constant: T) {
        self.lower += &constant;
        self.upper += &constant;
    }

    pub fn maybe_add(&mut self, maybe: &MaybeKnown<bool>, value: T) {
        match maybe {
            MaybeKnown::Unknown | MaybeKnown::UnknownExcluding(_) => {
                self.upper += &value;
            }
            MaybeKnown::Known(true) => {
                self.lower += &value;
                self.upper += &value;
            }
            MaybeKnown::Known(false) => {}
        }
    }

    // pub fn add_range(&mut self, other: &Self) {
    //     if let Some((prev_lower, prev_upper)) = &mut self.range {
    //         prev_lower.add_assign(lower);
    //         prev_upper.add_assign(upper);
    //     } else {
    //         self.range = Some((lower, upper));
    //     }
    // }
}

impl<T> Add<T> for RangeInclusive<T>
    where T: Add<Output = T> + Clone {
    type Output = RangeInclusive<T>;

    fn add(self, rhs: T) -> Self::Output {
        RangeInclusive {
            lower: self.lower + rhs.clone(),
            upper: self.upper + rhs,
        }
    }
}

#[derive(Debug)]
pub enum RangeDiff {
    NoDiff,
    UnexpectedChange(f32, f32),
    RerollOutsideRange(f32, f32, f32),
}

impl<T> PartialInformationCompare for RangeInclusive<T>
    where T: 'static + for<'de> Deserialize<'de> + Serialize + Debug + Send + Sync + Clone + PartialOrd {
    type Raw = T;
    type Diff<'d> = RangeDiff;

    fn diff<'d>(&'d self, _observed: &'d Self::Raw, _: DateTime<Utc>) -> Self::Diff<'d> {
        todo!()
    }

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        if observed < &self.lower || observed > &self.upper {
            if self.lower == self.upper {
                vec![Conflict::new(String::new(),
                                   format!("Expected {:?}, but observed {:?}",
                                           self.lower, observed))]
            } else {
                vec![Conflict::new(String::new(),
                                   format!("Expected value between {:?} and {:?}, but observed {:?}",
                                           self.lower, self.upper, observed))]
            }
        } else {
            self.upper = observed.clone();
            self.lower = observed.clone();
            Vec::new()
        }
    }

    fn is_ambiguous(&self) -> bool {
        self.lower < self.upper
    }

    fn from_raw(raw: Self::Raw) -> Self {
        Self { lower: raw.clone(), upper: raw }
    }

    fn raw_approximation(self) -> Self::Raw {
        self.lower // sure why not
    }
}

impl<'d> PartialInformationDiff<'d> for RangeDiff {
    fn is_empty(&self) -> bool {
        match self {
            RangeDiff::NoDiff => { true }
            _ => { false }
        }
    }
}