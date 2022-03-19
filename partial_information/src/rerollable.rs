use std::fmt::Debug;
use std::ops::AddAssign;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::PartialInformationCompare;

#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rerollable {
    raw: f32,
    range: Option<(f32, f32)>,
}
impl Rerollable {
    pub fn could_be(&self, other: f32) -> bool {
        if let Some((lower, upper)) = &self.range {
            (other >= &self.raw + lower) && (other <= &self.raw + upper)
        } else {
            &self.raw == &other
        }
    }

    pub fn add_constant(&mut self, constant: f32) {
        self.raw.add_assign(constant);
    }

    pub fn add_range(&mut self, lower: f32, upper: f32) {
        if let Some((prev_lower, prev_upper)) = &mut self.range {
            prev_lower.add_assign(lower);
            prev_upper.add_assign(upper);
        } else {
            self.range = Some((lower, upper));
        }
    }
}

#[derive(Debug)]
pub enum RerollableDiff {
    NoDiff,
    UnexpectedChange(f32, f32),
    RerollOutsideRange(f32, f32, f32),
}

impl PartialInformationCompare for Rerollable {
    type Raw = f32;
    type Diff<'d> = RerollableDiff;

    fn diff<'d>(&'d self, _observed: &'d Self::Raw, _: DateTime<Utc>) -> Self::Diff<'d> {
        todo!()
    }

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        if let Some((lower, upper)) = &self.range {
            if (observed >= &(&self.raw + lower)) && (observed <= &(&self.raw + upper)) {
                // Valid observation
                self.raw = observed.clone();
                self.range = None;
                Vec::new()
            } else {
                vec![Conflict::new(String::new(),
                                   format!("Expected value between {:?} and {:?}, but observed {:?}",
                                           &self.raw + lower, &self.raw + upper, observed))]
            }
        } else if &self.raw == observed {
            Vec::new()
        } else {
            vec![Conflict::new(String::new(),
                               format!("Expected value {:?}, but observed {:?}",
                                       self.raw, observed))]
        }
    }

    fn from_raw(raw: Self::Raw) -> Self {
        Self { raw, range: None }
    }

    fn raw_approximation(self) -> Self::Raw {
        self.raw
    }
}

impl<'d> PartialInformationDiff<'d> for RerollableDiff {
    fn is_empty(&self) -> bool {
        match self {
            RerollableDiff::NoDiff => { true }
            _ => { false }
        }
    }
}