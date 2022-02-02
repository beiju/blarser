use serde::Deserialize;
use partial_information::PartialInformationCompare;
use crate::api::EventuallyEvent;

pub enum FeedEventChangeResult {
    Ok,
    DidNotApply,
    Incompatible(String),
}

pub trait Entity: for<'de> Deserialize<'de> + PartialInformationCompare {
    fn new(json: serde_json::Value) -> Self where Self: Sized {
        serde_json::from_value(json)
            .expect("Error converting entity JSON to entity type")
    }

    fn apply_event(&mut self, event: &EventuallyEvent) -> FeedEventChangeResult;
}