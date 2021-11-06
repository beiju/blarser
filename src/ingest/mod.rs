use chrono::{DateTime, Utc};
use itertools::Itertools;

use chronicler_schema::ChroniclerItem;
use eventually_schema::EventuallyEvent;

pub mod eventually_schema;
pub mod chronicler;
pub mod chronicler_schema;
pub mod eventually;

pub enum IngestItem {
    FeedEvent(EventuallyEvent),
    ChronUpdate {
        endpoint: &'static str,
        item: ChroniclerItem,
    },
}

impl IngestItem {
    pub fn date(&self) -> DateTime<Utc> {
        match self {
            IngestItem::FeedEvent(e) => e.created,
            IngestItem::ChronUpdate { item, .. } => item.valid_from,
        }
    }
}

pub fn all(start: &'static str) -> impl Iterator<Item=IngestItem> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(|endpoint|
            Box::new(chronicler::versions(endpoint, start)
                .map(|item| IngestItem::ChronUpdate { endpoint, item }))
                as Box<dyn Iterator<Item=IngestItem>>
        )
        // Force the inner iterators to be started by collecting them, then turn the collection
        // right back into an iterator to continue the chain
        .collect::<Vec<Box<dyn Iterator<Item=IngestItem>>>>().into_iter()
        .chain([
            Box::new(eventually::events(start)
                .map(|event| IngestItem::FeedEvent(event)))
                as Box<dyn Iterator<Item=IngestItem>>
        ])
        .kmerge_by(|a, b| a.date() < b.date())
}