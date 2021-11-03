use std::rc::Rc;
use itertools::Itertools;

use crate::chronicler;
use crate::eventually;
use crate::blaseball_state::BlaseballState;
use crate::parse;
use crate::parse::{IngestEvent, IngestObject};

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub fn ingest() -> () {
    println!("Starting ingest");
    let mut latest_state = Rc::new(BlaseballState::from_chron_at_time(EXPANSION_ERA_START));
    println!("Got initial state");

    for object in merged_feed_and_chron() {
        match object {
            IngestObject::Event(event) => {
                latest_state = parse::apply_event(latest_state, event)
            }
            IngestObject::Update { endpoint, .. } => println!("Chron update: {}", endpoint),
        }
    };
}


pub fn merged_feed_and_chron() -> impl Iterator<Item=IngestObject> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(|endpoint|
            Box::new(chronicler::versions(endpoint, EXPANSION_ERA_START)
                .map(|item| IngestObject::Update { endpoint, item }))
                as Box<dyn Iterator<Item=IngestObject>>
        )
        // Force the inner iterators to be started by collecting them, then turn the collection
        // right back into an iterator to continue the chain
        .collect::<Vec<Box<dyn Iterator<Item=IngestObject>>>>().into_iter()
        .chain([
            Box::new(eventually::events(EXPANSION_ERA_START)
                .map(|event| IngestObject::Event(IngestEvent::FeedEvent(event))))
                as Box<dyn Iterator<Item=IngestObject>>
        ])
        .kmerge_by(|a, b| a.date() < b.date())
}