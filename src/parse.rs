use std::rc::Rc;
use chrono::{DateTime, Utc};
use crate::blaseball_state::{BlaseballState, Event, Uuid};
use crate::chronicler_schema::ChroniclerItem;
use crate::eventually_schema::{EventType, EventuallyEvent};

pub enum IngestObject {
    Event(IngestEvent),
    Update {
        endpoint: &'static str,
        item: ChroniclerItem,
    },
}

pub enum IngestEvent {
    FeedEvent(EventuallyEvent),
}


impl IngestObject {
    pub fn date(&self) -> DateTime<Utc> {
        match self {
            IngestObject::Event(IngestEvent::FeedEvent(e)) => e.created,
            IngestObject::Update { item, .. } => item.valid_from,
        }
    }
}

pub fn apply_event(state: Rc<BlaseballState>, event: IngestEvent) -> Rc<BlaseballState> {
    match event {
        IngestEvent::FeedEvent(event) => apply_feed_event(state, event)
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

    Rc::new(BlaseballState {
        predecessor: Some(state.clone()),
        from_event: Rc::new(Event::BigDeal {
            feed_event_id: Uuid::new(event.id)
        }),
        data: state.data.clone(),
    })
}
