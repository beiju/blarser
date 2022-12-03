use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::pin::Pin;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, stream, pin_mut};
use fed::FedEvent;
use futures::stream::Peekable;
use uuid::Uuid;
use serde_json::Value;
use itertools::{Either, Itertools};
use log::{info, log};
use crate::{entity, events};
use crate::events::{AnyEvent, Event, FeedEvent};
use crate::ingest::task::Ingest;
use crate::state::{Effects, EntityType, StateInterface, Version};

pub struct EventStreamItem {
    last_update_time: DateTime<Utc>,
    event: Option<AnyEvent>,
}

impl EventStreamItem {
    pub fn time(&self) -> DateTime<Utc> {
        if let Some(event) = &self.event {
            std::cmp::min(event.time(), self.last_update_time)
        } else {
            self.last_update_time
        }
    }

    pub fn last_update_time(&self) -> DateTime<Utc> {
        self.last_update_time
    }

    pub fn event(&self) -> &Option<AnyEvent> {
        &self.event
    }

    pub fn into_event(self) -> Option<AnyEvent> {
        self.event
    }
}

pub fn get_fed_event_stream() -> impl Stream<Item=EventStreamItem> {
    // This is temporary, eventually it will be an HTTP call
    let fed_up_to_date_until = DateTime::parse_from_rfc3339(fed::EXPANSION_ERA_END)
        .expect("Couldn't parse hard-coded Blarser start time")
        .with_timezone(&Utc);

    let iter = fed::expansion_era_events()
        .map(move |event| EventStreamItem {
            last_update_time: fed_up_to_date_until,
            event: Some(FeedEvent::any_from_fed(event.unwrap())),
        });

    stream::iter(iter)
}

pub async fn get_timed_event_list(ingest: &mut Ingest, start_time: DateTime<Utc>) -> BinaryHeap<Reverse<AnyEvent>> {
    // TODO Add other sources of timed events
    let sim_versions: Vec<Version<entity::Sim>> = ingest.run(move |mut state: StateInterface| {
        state.get_versions_at::<entity::Sim>(EntityType::Sim, None, start_time)
    }).await
        .expect("Failed to read versions to init timed events");

    let events = if let Some((sim_version, )) = sim_versions.into_iter().collect_tuple() {
        let sim: entity::Sim = sim_version.entity;
        if sim.phase == 1 && sim.earlseason_date > start_time {
            vec![AnyEvent::EarlseasonStart(events::EarlseasonStart::new(sim.earlseason_date))]
        } else {
            todo!()
        }
    } else {
        panic!("Expected there to be exactly one Sim version")
    };

    BinaryHeap::from(events.into_iter().map(Reverse).collect::<Vec<_>>())
}


pub fn ingest_event(ingest: &mut Ingest, event: AnyEvent) -> Vec<AnyEvent> {
    info!("Ingesting event {event}");
    todo!()
}