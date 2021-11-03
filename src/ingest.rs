use std::error::Error;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;
use chrono::{DateTime, Utc};
use itertools::Itertools;

use crate::chronicler;
use crate::chronicler_schema::ChroniclerItem;
use crate::eventually;
use crate::eventually_schema::{EventType, EventuallyEvent};
use crate::blaseball_state::{BlaseballState, Event, Uuid};
use crate::chronicler::ENDPOINT_NAMES;

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub enum UnifiedEvent {
    FeedEvent(EventuallyEvent),
}

pub enum IngestObject {
    Event(UnifiedEvent),
    Update {
        endpoint: &'static str,
        item: ChroniclerItem,
    },
}

impl IngestObject {
    fn date(&self) -> DateTime<Utc> {
        match self {
            IngestObject::Event(UnifiedEvent::FeedEvent(e)) => e.created,
            IngestObject::Update {  item, .. } => item.valid_from,
        }
    }
}

pub fn ingest() -> Result<(), impl Error> {
    println!("Starting ingest");
    let mut latest_state = Rc::new(BlaseballState::from_chron_at_time(EXPANSION_ERA_START));
    println!("Got initial state");

    loop {
        match merged_feed_and_chron().recv() {
            Ok(IngestObject::Event(event)) => {
                latest_state = apply_event(latest_state, event)
            },
            Ok(IngestObject::Update { endpoint, .. }) => println!("Chron update: {}", endpoint),
            Err(e) => return Err(e),
        }
    };
}

fn apply_event(state: Rc<BlaseballState>, event: UnifiedEvent) -> Rc<BlaseballState> {
    match event {
        UnifiedEvent::FeedEvent(event) => apply_feed_event(state, event)
    }
}

fn apply_feed_event(state: Rc<BlaseballState>, event: EventuallyEvent) -> Rc<BlaseballState> {
    println!("Applying event: {}", event.description);

    match event.r#type {
        EventType::BigDeal => apply_big_deal(state, event),
        _ => todo!()
    }
}

fn apply_big_deal(state: Rc<BlaseballState>, event: EventuallyEvent) -> Rc<BlaseballState> {
    println!("Applying BigDeal event");

    Rc::new(BlaseballState{
        predecessor: Some(state.clone()),
        from_event: Rc::new(Event::BigDeal {
            feed_event_id: Uuid::new(event.id)
        }),
        data: state.data.clone()
    })
}

pub fn merged_feed_and_chron() -> mpsc::Receiver<IngestObject> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || ingest_thread(sender));
    receiver
}

fn ingest_thread(sender: mpsc::SyncSender<IngestObject>) -> () {
    let sources_merged = ENDPOINT_NAMES.into_iter()
        .map(|endpoint|
            Box::new(chronicler::versions(endpoint, EXPANSION_ERA_START)
                .map(|item| IngestObject::Update { endpoint, item }))
                as Box<dyn Iterator<Item=IngestObject>>
        )
        .chain([
            Box::new(eventually::events(EXPANSION_ERA_START)
                .map(|event| IngestObject::Event(UnifiedEvent::FeedEvent(event))))
                as Box<dyn Iterator<Item=IngestObject>>
        ])
        .kmerge_by(|a, b| a.date() < b.date());

    for item in sources_merged {
        sender.send(item).unwrap();
    }
}
