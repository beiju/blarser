use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Utc};
use diesel::{Connection, PgConnection};
use futures::{pin_mut, stream, Stream, StreamExt};
use rocket::{info};
use uuid::Uuid;
use itertools::Itertools;

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;
use crate::{sim, EntityStateInterface};
use crate::state::{ChronObservationEvent, Event, MergedSuccessors, add_chron_event, add_initial_versions, get_events_for_entity_after, delete_versions_for_entity_after, get_current_versions, save_versions, terminate_versions};
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


type ChronUpdateStream = Pin<Box<dyn Stream<Item=(&'static str, ChroniclerItem)> + Send>>;

fn chron_updates(start_at_time: &'static str) -> impl Stream<Item=(&'static str, ChroniclerItem)> {
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                .map(move |entity| (entity_type, entity))
                .peekable();

            Box::pin(stream) as ChronUpdateStream
        })
        .chain(iter::once(
            Box::pin(chronicler::game_updates(start_at_time)
                .map(move |entity| ("game", entity))
                .peekable()
            ) as ChronUpdateStream
        ));

    kmerge_stream(streams)
}

fn kmerge_stream(streams: impl Iterator<Item=ChronUpdateStream>) -> impl Stream<Item=(&'static str, ChroniclerItem)> {
    let peekable_streams: Vec<_> = streams
        // Two layers of Box::pin :(
        .map(|s| Box::pin(s.peekable()))
        .collect();

    stream::unfold(peekable_streams, |mut peekable_streams| async {
        let selected_stream = *stream::iter(&mut peekable_streams)
            .enumerate()
            .filter_map(|(i, stream)| async move {
                if let Some((_, next_item)) = stream.as_mut().peek().await {
                    Some((i, next_item.valid_from))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>().await
            .iter()
            .min_by_key(|(_, date)| date)
            .map(|(i, _)| i)
            .expect("TODO: Handle end of all streams");

        let next = peekable_streams[selected_stream].next().await
            .expect("selected_stream should never refer to a stream that doesn't have a next element");

        Some((next, peekable_streams))
    })
}

pub async fn init_chron(ingest: &mut IngestState, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    let initial_versions: Vec<_> = initial_state(start_at_time).collect().await;
    add_initial_versions(&mut ingest.db, ingest.ingest_id, start_time_parsed, initial_versions).await;

    info!("Finished populating initial Chron values");
}

pub async fn ingest_chron(mut ingest: IngestState, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    let updates = chron_updates(start_at_time);

    pin_mut!(updates);

    while let Some((entity_type, item)) = updates.next().await {
        entity_dispatch!(entity_type => ingest_update(&mut ingest, item).await;
                         other => panic!("Unsupported entity type {}", other));
    }
}

async fn ingest_update<EntityT: 'static + sim::Entity>(ingest: &mut IngestState, item: ChroniclerItem) {
    let entity_raw: EntityT::Raw = serde_json::from_value(item.data)
        .expect("Error deserializing raw entity");
    info!("Processing chron update for {} {} at {}", EntityT::name(), item.entity_id, item.valid_from);

    // Necessary to avoid capturing ingest in the transaction closure
    let ingest_id = ingest.ingest_id;
    let (earliest, latest) = EntityT::time_range_for_update(item.valid_from, &entity_raw);
    wait_for_feed_ingest(ingest, latest).await;
    ingest.db.run(move |c| {
        c.transaction(|| {
            do_ingest::<EntityT>(c, ingest_id, earliest, latest, item.valid_from, item.entity_id, entity_raw);

            Ok::<_, diesel::result::Error>(())
        })
    }).await.unwrap();
}

fn do_ingest<EntityT: 'static + sim::Entity>(
    c: &PgConnection,
    ingest_id: i32,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    perceived_at: DateTime<Utc>,
    entity_id: Uuid,
    entity_raw: EntityT::Raw
) {
    info!("Placing {} {} between {} and {}", EntityT::name(), entity_id, start_time, end_time);

    // The order for this is important! First get events by reading the versions after start_time,
    // then delete the versions after start_time, then get the leaf versions which will now be the
    // exact set of versions we need to start the ingest from.
    let events = get_events_for_entity_after(c, ingest_id, EntityT::name(), entity_id, start_time)
        .expect("Error getting events for Chronicler ingest");

    delete_versions_for_entity_after(c, ingest_id, EntityT::name(), entity_id, start_time)
        .expect("Error deleting to-be-replaced versions");

    let mut versions: Vec<_> = get_current_versions(c, ingest_id, EntityT::name(), Some(entity_id))
        .into_iter()
        .map(|(version_id, value, version_time)| {
            assert!(version_time <= start_time);
            let entity: EntityT = serde_json::from_value(value)
                .expect("Couldn't parse stored version");
            (false, version_id, entity)
        })
        .collect();
    info!("Applying {} events to {} versions", events.len(), versions.len());

    let mut observation_event = ChronObservationEvent {
        entity_type: EntityT::name().to_string(),
        entity_id,
        perceived_at,
        applied_at: start_time,
    };

    let mut event_id = add_chron_event(c, ingest_id, observation_event.clone());

    for event in events {
        let event_time = event.event_time;
        versions = advance_version(c, ingest_id, versions, event, event_id, &entity_raw, entity_id, observation_event.applied_at, end_time);
        observation_event.applied_at = event_time;
        event_id = add_chron_event(c, ingest_id, observation_event.clone());
    }

    let mut any_applied = false;
    if observation_event.applied_at < end_time {
        // Now need to apply to the latest version, after all events in this time range
        let mut last_successors = MergedSuccessors::new();
        for (already_applied, version_id, mut entity) in versions {
            if already_applied {
                any_applied = true;
            } else {
                let conflicts = entity.observe(&entity_raw);
                if conflicts.is_empty() {
                    any_applied = true;
                    last_successors.add_successor(version_id, entity);
                } else {
                    info!("Not applying observation because of conflicts: \n- {}", conflicts.into_iter().map(|c| c.to_string()).join("\n- "))
                }
            }
        }

        save_versions(c, ingest_id, event_id, observation_event.applied_at, last_successors.into_inner());
    } else {
        any_applied = versions.iter().any(|(applied, _, _)| *applied);
    }

    if !any_applied {
        // Throw up an alert -- this Chron update couldn't be applied at all
        todo!()
    }

    info!("Finished ingest for {} {}", EntityT::name(), entity_id);
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

fn advance_version<EntityT: 'static + sim::Entity>(
    c: &PgConnection,
    ingest_id: i32,
    entities: Vec<(bool, i32, EntityT)>,
    event: Event,
    update_id: i32,
    entity_raw: &EntityT::Raw,
    entity_id: Uuid,
    earliest_time: DateTime<Utc>,
    end_time: DateTime<Utc>
) -> Vec<(bool, i32, EntityT)> {
    let mut new_entities = Vec::new();

    // Save for use after moving event
    let event_id = event.id;
    let event_time = event.event_time;

    let mut versions_from_observation = MergedSuccessors::new();
    let mut to_terminate = Vec::new();
    for (observation_already_applied, version_id, entity) in entities {
        // If we haven't already applied the observation, and it's valid to apply the observation
        // here, add the branch where we apply the observation here
        if !observation_already_applied {
            let mut entity_after_observation = entity.clone();
            let conflicts = entity_after_observation.observe(entity_raw);
            if conflicts.is_empty() {
                versions_from_observation.add_successor(version_id, entity_after_observation);
            } else {
                info!("Not applying observation because of conflicts: \n- {}", conflicts.into_iter().map(|c| c.to_string()).join("\n- "))
            }
        }

        if event_time > end_time && !observation_already_applied {
            // Terminate the branch
            to_terminate.push(version_id);
        } else {
            // Always add the branch where we don't apply the observation
            new_entities.push((observation_already_applied, version_id, entity));
        }
    }
    if !to_terminate.is_empty() {
        // TODO Put the non-termination reasons in the string
        terminate_versions(c, to_terminate,
                           format!("This branch didn't apply a chron update at any point"));
    }
    // NOTE: At this point, new_entities doesn't yet contain any of the branches that came from
    // applying the observation, because we haven't saved them to the DB and don't have their ids
    // yet.
    let saved_version_ids = save_versions(c, ingest_id, update_id, earliest_time, versions_from_observation.clone().into_inner());
    new_entities.extend(
        versions_from_observation.into_inner().into_iter()
                .zip_eq(saved_version_ids)
                .map(|((entity, _), id)| (true, id, entity))
    );

    let mut state: EntityStateInterface<EntityT> = EntityStateInterface::new(c, ingest_id, event_time, entity_id, new_entities);
    event.apply(&mut state);

    let successors = state.get_successors();
    let (successors, successors_to_save): (Vec<_>, Vec<_>) = successors.into_iter()
        .map(|((applied, successor), parent_ids)| {
            (
                (applied, successor.clone()), // These ones are saved for the next iteration
                (successor, parent_ids), // These ones are passed to the database function
            )
        })
        .unzip();

    info!("Saving {} successors for {} {}", successors_to_save.len(), EntityT::name(), entity_id);
    let successor_ids = save_versions(c, ingest_id, event_id, event_time, successors_to_save);

    successors.into_iter().zip_eq(successor_ids)
        .map(|((applied, version), version_id)| (applied, version_id, version))
        .collect()
}