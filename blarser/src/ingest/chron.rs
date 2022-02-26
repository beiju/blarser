use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Duration, Utc};
use diesel::{Connection, PgConnection};
use futures::{pin_mut, stream, Stream, StreamExt};
use rocket::{info};
use uuid::Uuid;
use itertools::Itertools;

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;
use crate::schema::events::star;
use crate::{sim, StateInterface};
use crate::state::{add_initial_versions, Event, get_events_for_entity_after, get_possible_versions_at};
use crate::sim::entity_dispatch;

fn initial_state(start_at_time: &'static str) -> impl Stream<Item=(&'static str, ChroniclerItem)> {
    type ChronUpdateStream = Pin<Box<dyn Stream<Item=(&'static str, ChroniclerItem)> + Send>>;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::entities(entity_type, start_at_time)
                .map(move |entity| (entity_type, entity));

            Box::pin(stream) as ChronUpdateStream
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |entity| ("game", entity))
            ) as ChronUpdateStream
        ));

    stream::select_all(streams)
}


fn chron_updates(start_at_time: &'static str) -> impl Stream<Item=(&'static str, ChroniclerItem)> {
    type ChronUpdateStream = Pin<Box<dyn Stream<Item=(&'static str, ChroniclerItem)> + Send>>;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                .map(move |entity| (entity_type, entity));

            Box::pin(stream) as ChronUpdateStream
        })
        .chain(iter::once(
            Box::pin(chronicler::game_updates(start_at_time)
                .map(move |entity| ("game", entity))
            ) as ChronUpdateStream
        ));

    stream::select_all(streams)
}

pub async fn init_chron(ingest: &mut IngestState, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    let initial_versions: Vec<_> = initial_state(start_at_time).collect().await;
    add_initial_versions(&mut ingest.db, ingest.ingest_id, start_time_parsed, initial_versions).await;

    info!("Finished populating initial Chron values");
}

pub async fn ingest_chron(mut ingest: IngestState, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    info!("Started Chron ingest task");

    let updates = chron_updates(start_at_time);

    pin_mut!(updates);

    while let Some((entity_type, item)) = updates.next().await {
        entity_dispatch!(entity_type => ingest_update(&mut ingest, item).await;
                         other => panic!("Unsupported entity type {}", other));
    }
}

async fn ingest_update<EntityT: sim::Entity>(ingest: &mut IngestState, item: ChroniclerItem) {
    let entity_raw: EntityT::Raw = serde_json::from_value(item.data)
        .expect("Error deserializing raw entity");
    info!("Processing chron update for {} {} at {}", EntityT::name(), item.entity_id, item.valid_from);

    // Necessary to avoid capturing ingest in the transaction closure
    let ingest_id = ingest.ingest_id;
    let (earliest, latest) = EntityT::time_range_for_update(item.valid_from, &entity_raw);
    wait_for_feed_ingest(ingest, latest).await;
    ingest.db.run(move |c| {
        c.transaction(|| {
            do_ingest::<EntityT>(c, ingest_id, earliest, item.entity_id, entity_raw);

            Ok::<_, diesel::result::Error>(())
        })
    }).await.unwrap();

    todo!()
}

fn do_ingest<EntityT: sim::Entity>(c: &PgConnection, ingest_id: i32, start_time: DateTime<Utc>, entity_id: Uuid, entity_raw: EntityT::Raw) {
    let mut versions: Vec<EntityT> = get_possible_versions_at(c, ingest_id, EntityT::name(), Some(entity_id), start_time)
        .into_iter()
        .map(|(_, value, _)| {
            serde_json::from_value(value)
                .expect("Couldn't parse stored version")
        })
        .collect();

    let events = get_events_for_entity_after(c, ingest_id, EntityT::name(), entity_id, start_time)
        .expect("Error getting events for Chronicler ingest");

    for event in events {
        versions = advance_version(versions, event, &entity_raw);
    }

    // TODO: If no applications were found (cant_apply_reason is still Some()), prompt the user to
    //   approve a manual change. Otherwise, delete the old successors and insert the newly-computed
    //   chain.
    todo!()
}

async fn wait_for_feed_ingest(ingest: &mut IngestState, wait_until_time: DateTime<Utc>) {
    ingest.notify_progress.send(wait_until_time)
        .expect("Error communicating with Chronicler ingest");
    info!("Chron ingest sent {} as requested time", wait_until_time);

    loop {
        let feed_time = *ingest.receive_progress.borrow();
        if wait_until_time < feed_time {
            break;
        }
        info!("Chronicler ingest waiting for Eventually ingest to catch up (at {} and we need {}, difference of {}s)",
            feed_time, wait_until_time, (wait_until_time - feed_time).num_seconds());
        ingest.receive_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}

fn advance_version<EntityT: sim::Entity>(c: &PgConnection, ingest_id: i32, entities: Vec<(bool, EntityT)>, event: Event, entity_raw: &EntityT::Raw) -> Vec<(bool, EntityT)> {
    let mut new_entities = Vec::new();
    for (observation_already_applied, mut entity) in entities {
        // If we haven't already applied the observation, and it's valid to apply the observation
        // here, add the branch where we apply the observation here
        if !observation_already_applied {
            let conflicts = entity.observe(entity_raw);
            if conflicts.is_empty() {
                new_entities.push((true, entity.clone()))
            }
        }

        // Always add the branch where we don't apply the observation
        new_entities.push((observation_already_applied, entity));
    }
    let state = StateInterface::for_entity(c, ingest_id, event.id, event.event_time, EntityT::name(), entity.id());
    event.apply(&mut entity);


    new_entities
}