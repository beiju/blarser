use std::error::Error;
use std::sync::mpsc;
use std::thread;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use crate::chronicler;
use crate::chronicler_schema::ChroniclerItem;
use crate::eventually;
use crate::eventually_schema::EventuallyEvent;
use crate::ingest::IngestObject::{FeedEvent, ChronPlayerUpdate, ChronTeamUpdate};
use crate::blaseball_state::BlaseballState;

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub enum IngestObject {
    FeedEvent(EventuallyEvent),
    ChronPlayerUpdate(ChroniclerItem),
    ChronTeamUpdate(ChroniclerItem),
}

impl IngestObject {
    // fn date(&self) -> DateTime<Utc> {
    //     match self {
    //         FeedEvent(e) => e.created,
    //         ChronPlayerUpdate(u) => u.valid_from,
    //         ChronTeamUpdate(u) => u.valid_from,
    //     }
    // }
}

pub fn ingest() -> Result<(), Box<dyn Error>> {
    let latest_state = BlaseballState::from_chron_at_time(EXPANSION_ERA_START);

    for (name, values) in latest_state.data.into_iter() {
        println!("Endpoint {} had {} values", name, values.len());
    }

    // let recv = merged_feed_and_chron();
    //
    // loop {
    //     match recv.recv() {
    //         Ok(FeedEvent(_)) => println!("Event"),
    //         Ok(ChronPlayerUpdate(_)) => println!("Players"),
    //         Ok(ChronTeamUpdate(_)) => println!("Teams"),
    //         Err(e) => return Err(e),
    //     }
    // };
    Ok(())
}

pub fn merged_feed_and_chron() -> mpsc::Receiver<IngestObject> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || ingest_thread(sender) );
    receiver
}

fn ingest_thread(sender: mpsc::SyncSender<IngestObject>) -> () {
    let events_recv = eventually::events(EXPANSION_ERA_START);
    let players_recv = chronicler::versions("player", EXPANSION_ERA_START);
    let teams_recv = chronicler::versions("team", EXPANSION_ERA_START);

    // TODO Can this be less let-mut-y
    let mut events_iter = events_recv.into_iter().map(|event| IngestObject::FeedEvent(event));
    let mut players_iter = players_recv.into_iter().map(|update| IngestObject::ChronPlayerUpdate(update));
    let mut teams_iter = teams_recv.into_iter().map(|update| IngestObject::ChronTeamUpdate(update));
    let sources = vec![
        &mut events_iter as &mut dyn Iterator<Item=IngestObject>,
        &mut players_iter as &mut dyn Iterator<Item=IngestObject>,
        &mut teams_iter as &mut dyn Iterator<Item=IngestObject>,
    ];

    // let sources_merged = sources.into_iter()
    //     .kmerge_by(|a, b| a.date() < b.date());
    //
    // for item in sources_merged {
    //     sender.send(item);
    // }
}
