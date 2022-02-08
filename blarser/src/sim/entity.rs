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

    fn next_timed_event(&self, from_time: DateTime<Utc>, to_time: DateTime<Utc>, state: &StateInterface) -> Option<GenericEvent>;

    fn apply_event(&mut self, event: &GenericEvent, state: &StateInterface) -> FeedEventChangeResult;
}

pub struct EarliestEvent(Option<GenericEvent>);

impl EarliestEvent {
    pub fn new() -> EarliestEvent {
        EarliestEvent(None)
    }

    pub fn push(&mut self, new_event: GenericEvent) {
        match &self.0 {
            None => {
                self.0 = Some(new_event)
            }
            Some(prev_event) if prev_event.time >= new_event.time => {
                assert_ne!(prev_event.time, new_event.time,
                           "state.versions() doesn't work properly if multiple timed events fire at the same time");
                self.0 = Some(new_event)
            }
            _ => {}
        }
    }

    pub fn push_opt(&mut self, opt: Option<GenericEvent>) {
        if let Some(event) = opt {
            self.push(event);
        }
    }

    pub fn into_inner(self) -> Option<GenericEvent> {
        self.0
    }
}
