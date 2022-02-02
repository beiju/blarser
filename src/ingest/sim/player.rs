use serde::Deserialize;
use crate::api::EventuallyEvent;
use crate::ingest::sim::{Entity, FeedEventChangeResult};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Player {}

impl Entity for Player {
    fn apply_event(self, event: &EventuallyEvent) -> FeedEventChangeResult<Self> where Self: Sized {
        todo!()
    }

    fn could_be(&self, other: &Self) -> bool {
        todo!()
    }
}