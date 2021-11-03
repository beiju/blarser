use std::error::Error;
use std::sync::mpsc;
use std::thread;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use crate::chronicler;
use crate::chronicler_schema::ChroniclerItem;
use crate::eventually;
use crate::eventually_schema::EventuallyEvent;
use crate::blaseball_state::BlaseballState;

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub enum IngestObject {
    FeedEvent(EventuallyEvent),
    ChronUpdate {
        endpoint: &'static str,
        item: ChroniclerItem,
    },
}

impl IngestObject {
    fn date(&self) -> DateTime<Utc> {
        match self {
            IngestObject::FeedEvent(e) => e.created,
            IngestObject::ChronUpdate { endpoint, item } => item.valid_from,
        }
    }
}

pub fn ingest() -> Result<(), impl Error> {
    let latest_state = BlaseballState::from_chron_at_time(EXPANSION_ERA_START);

    for (name, values) in latest_state.data.into_iter() {
        println!("Endpoint {} had {} values", name, values.len());
    }

    let recv = merged_feed_and_chron();

    loop {
        match recv.recv() {
            Ok(IngestObject::FeedEvent(_)) => println!("Event"),
            Ok(IngestObject::ChronUpdate { endpoint, .. }) => println!("Chron update: {}", endpoint),
            Err(e) => return Err(e),
        }
    };
}

pub fn merged_feed_and_chron() -> mpsc::Receiver<IngestObject> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || ingest_thread(sender));
    receiver
}

fn ingest_thread(sender: mpsc::SyncSender<IngestObject>) -> () {
    let sources_merged = ["player", "team"].into_iter()
        .map(|endpoint|
            Box::new(chronicler::versions(endpoint, EXPANSION_ERA_START)
                .map(|item| IngestObject::ChronUpdate { endpoint, item }))
                as Box<dyn Iterator<Item=IngestObject>>
        )
        .chain([
            Box::new(eventually::events(EXPANSION_ERA_START)
                .map(|event| IngestObject::FeedEvent(event)))
                as Box<dyn Iterator<Item=IngestObject>>
        ])
        .kmerge_by(|a, b| a.date() < b.date());

    for item in sources_merged {
        sender.send(item);
    }
}
