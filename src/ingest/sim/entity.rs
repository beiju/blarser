use serde::Deserialize;
use crate::api::EventuallyEvent;

pub enum FeedEventChangeResult<EntityT> {
    DidNotApply,
    Incompatible,
    Ok(EntityT)
}

pub trait Entity: for<'de> Deserialize<'de> {
    fn new(json: serde_json::Value) -> Self where Self: Sized {
        serde_json::from_value(json)
            .expect("Error converting entity JSON to entity type")
    }

    fn apply_event(self, event: &EventuallyEvent) -> FeedEventChangeResult<Self> where Self: Sized;
    fn could_be(&self, other: &Self) -> bool;
}