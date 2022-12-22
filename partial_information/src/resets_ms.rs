use std::fmt::Debug;
use chrono::{DateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use crate::compare::{Conflict, PartialInformationDiff};
use crate::{MaybeKnown, PartialInformationCompare};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatetimeWithResettingMs {
    date: DateTime<Utc>,
    ms_known: bool,
}

impl DatetimeWithResettingMs {
    pub fn date(&self) -> DateTime<Utc> {
        self.date
    }
    pub fn known_date(&self) -> Option<DateTime<Utc>> {
        if self.ms_known {
            Some(self.date)
        } else {
            None
        }
    }

    pub fn from_without_ms(date: DateTime<Utc>) -> Self {
        Self {
            date,
            ms_known: false,
        }
    }

    pub fn forget_ms(&mut self) {
        self.ms_known = false;
    }

    pub fn ns(&self) -> MaybeKnown<u32> {
        if self.ms_known {
            MaybeKnown::Known(self.date.nanosecond())
        } else {
            MaybeKnown::Unknown
        }
    }

    pub fn set_ns(&mut self, ns: u32) {
        self.date = self.date.with_nanosecond(ns).unwrap();
        self.ms_known = true;
    }
}

impl From<DateTime<Utc>> for DatetimeWithResettingMs {
    fn from(date: DateTime<Utc>) -> Self {
        Self {
            date,
            ms_known: true
        }
    }
}

#[derive(Debug)]
pub enum ResetsMsDiff {
    NoDiff,
    Diff(DateTime<Utc>),
}

impl PartialInformationCompare for DatetimeWithResettingMs {
    type Raw = DateTime<Utc>;
    type Diff<'d> = ResetsMsDiff;

    fn diff<'d>(&'d self, _observed: &'d Self::Raw, _: DateTime<Utc>) -> Self::Diff<'d> {
        todo!("What's the expected behavior if self.ms_known is false?")
        // if &self.date == observed {
        //     ResetsMsDiff::NoDiff
        // } else {
        //     ResetsMsDiff::Diff(*observed)
        // }
    }

    fn observe(&mut self, observed: &Self::Raw) -> Vec<Conflict> {
        if self.ms_known {
            if &self.date == observed {
                Vec::new()
            } else {
                vec![Conflict::new(String::new(),
                                   format!("Expected exactly {} but got {}", self.date, observed))]
            }
        } else {
            let actual_no_ms = self.date.with_nanosecond(0).unwrap();
            let observed_no_ms = observed.with_nanosecond(0).unwrap();
            if actual_no_ms == observed_no_ms {
                self.date = *observed;
                self.ms_known = true;
                Vec::new()
            } else {
                vec![Conflict::new(String::new(),
                                   format!("Expected {} but got {}", actual_no_ms, observed_no_ms))]
            }
        }
    }

    fn is_ambiguous(&self) -> bool {
        !self.ms_known
    }

    fn from_raw(raw: Self::Raw) -> Self {
        Self {
            date: raw,
            ms_known: true,
        }
    }

    fn raw_approximation(self) -> Self::Raw {
        self.date
    }
}

impl<'d> PartialInformationDiff<'d> for ResetsMsDiff {
    fn is_empty(&self) -> bool {
        match self {
            ResetsMsDiff::NoDiff => { true }
            ResetsMsDiff::Diff(_) => { false }
        }
    }
}