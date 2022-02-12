use std::fmt::Debug;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use partial_information::PartialInformationCompare;
use crate::state::{GenericEvent, StateInterface};

pub enum FeedEventChangeResult {
    Ok,
    DidNotApply,
}

pub trait Entity: for<'de> Deserialize<'de> + PartialInformationCompare + Clone + Debug {
    fn name() -> &'static str;

    fn new(json: serde_json::Value) -> Self::Raw where Self::Raw: Sized {
        serde_json::from_value(json)
            .expect("Error converting entity JSON to entity type")
    }

    fn next_timed_event(&self, after_time: DateTime<Utc>) -> Option<DateTime<Utc>>;

    fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult;
}

// Helper used in next_timed_event
pub struct Lowest<T: PartialOrd> {
    limit: T,
    lowest: Option<T>
}

impl<T:PartialOrd> Lowest<T> {
    pub fn new(limit: T) -> Lowest<T> {
        Lowest { limit, lowest: None }
    }

    pub fn push(&mut self, new_val: T) {
        if new_val < self.limit { return }

        match &self.lowest {
            None => {
                self.lowest = Some(new_val)
            }
            Some(prev_event) if &new_val < prev_event => {
                self.lowest = Some(new_val)
            }
            _ => {}
        }
    }

    pub fn push_opt(&mut self, opt: Option<T>) {
        if let Some(event) = opt {
            self.push(event);
        }
    }

    pub fn into_inner(self) -> Option<T> {
        self.lowest
    }
}
