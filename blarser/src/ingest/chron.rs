use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Utc};
use diesel::{Connection, PgConnection};
use futures::{pin_mut, stream, Stream, StreamExt};
use rocket::{info};
use uuid::Uuid;
use itertools::Itertools;
use tokio::sync::oneshot;
use thiserror::Error;
use partial_information::{Conflict, PartialInformationCompare};

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;
use crate::{sim, EntityStateInterface};
use crate::ingest::approvals_db::{ApprovalState, get_approval};
use crate::state::{ChronObservationEvent, Event, MergedSuccessors, add_chron_event, add_initial_versions, get_events_for_entity_after, delete_versions_for_entity_after, get_current_versions, save_versions_from_entities, terminate_versions, get_possible_versions_at, get_entity_update_tree, Version, Parent, NewVersion, save_versions};
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

#[derive(Debug, Error)]
enum IngestError {
    #[error("Needs approval: {0:?}")]
    NeedsApproval(Vec<(i32, String)>),

    #[error(transparent)]
    DieselError(#[from] diesel::result::Error),
}

async fn ingest_update<EntityT: 'static + sim::Entity>(ingest: &mut IngestState, item: ChroniclerItem) {
    let entity_raw: EntityT::Raw = serde_json::from_value(item.data)
        .expect("Error deserializing raw entity");
    info!("Processing chron update for {} {} at {}", EntityT::name(), item.entity_id, item.valid_from);

    // Necessary to avoid capturing ingest in the transaction closure
    let ingest_id = ingest.ingest_id;
    let (earliest, latest) = EntityT::time_range_for_update(item.valid_from, &entity_raw);
    wait_for_feed_ingest(ingest, latest).await;

    let _lock = ingest.damn_mutex.lock().await;
    // I can't find a way to do a proper borrow of entity_raw, so I'm just moving it through the
    // async tree
    let (approval, entity_raw) = ingest.db.run(move |c| {
        let approval_result = c.transaction(|| {
            let conflicts = do_ingest::<EntityT>(
                c,
                ingest_id,
                earliest,
                latest,
                item.valid_from,
                item.entity_id,
                &entity_raw,
                false
            );

            // Round-trip through the Result machinery to get diesel to cancel the transaction
            match conflicts {
                None => { Ok(()) }
                Some(c) => { Err(IngestError::NeedsApproval(c)) }
            }
        });

        if let Err(IngestError::NeedsApproval(approval)) = approval_result {
            (Some(approval), entity_raw)
        } else {
            approval_result.expect("Unexpected database error in chronicler ingest");
            (None, entity_raw)
        }
    }).await;

    if let Some(conflicts) = approval {
        // TODO Make a fun html debug view from conflicts info
        let message = conflicts.into_iter()
            .map(|(id, reason)| {
                format!("Can't apply observation to version {}:\n{}", id, reason)
            })
            .join("\n");
        let approval = ingest.db.run(move |c| {
            get_approval(c, EntityT::name(), item.entity_id, item.valid_from, &message)
        }).await
            .expect("Error saving approval to db");

        let approved = match approval {
            ApprovalState::Pending(approval_id) => {
                let (send, recv) = oneshot::channel();
                {
                    let mut pending_approvals = ingest.pending_approvals.lock().unwrap();
                    pending_approvals.insert(approval_id, send);
                }
                recv.await
                    .expect("Channel closed while awaiting approval")
            }
            ApprovalState::Approved(_) => { true }
            ApprovalState::Rejected => { false }
        };

        if approved {
            ingest.db.run(move |c| {
                c.transaction(|| {
                    do_ingest::<EntityT>(c, ingest_id, earliest, latest, item.valid_from, item.entity_id, &entity_raw, true)
                        .expect("Ingest failed even with force=true");

                    Ok::<_, diesel::result::Error>(())
                })
            }).await.unwrap();
        } else {
            panic!("Approval rejected")
        }
    }
}

fn do_ingest<EntityT: 'static + sim::Entity>(
    c: &PgConnection,
    ingest_id: i32,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    perceived_at: DateTime<Utc>,
    entity_id: Uuid,
    entity_raw: &EntityT::Raw,
    force: bool,
) -> Option<Vec<(i32, String)>> {
    info!("Placing {} {} between {} and {}", EntityT::name(), entity_id, start_time, end_time);

    let (events, generations) = get_entity_update_tree(c, ingest_id, EntityT::name(), entity_id, start_time)
        .expect("Error getting events for Chronicler ingest");

    let mut to_terminate = None;

    let mut prev_generation = Vec::new();
    let mut version_conflicts = Some(Vec::new());
    for (event, versions) in events.into_iter().zip(generations) {
        let mut new_generation = MergedSuccessors::new();

        if event.event_time <= end_time {
            to_terminate = Some(versions.iter().map(|(v, _)| v.id).collect());
            observe_generation::<EntityT>(&mut new_generation, &mut version_conflicts, versions, entity_raw, perceived_at);
        }

        advance_generation(c, ingest_id, &mut new_generation, EntityT::name(), entity_id, event, prev_generation);

        prev_generation = save_versions(c, new_generation.into_inner())
            .expect("Error saving updated versions");
    }

    if let Some(to_terminate) = to_terminate {
        terminate_versions(c, to_terminate,format!("Failed to apply observation at {}", perceived_at))
                .expect("Failed to terminate versions");
    }

    version_conflicts
}

fn advance_generation(c: &PgConnection, ingest_id: i32, new_generation: &mut MergedSuccessors<NewVersion>, entity_type: &'static str, entity_id: Uuid, event: Event, prev_generation: Vec<Version>) {
    let event_time = event.event_time;
    let from_event = event.id;
    let event = event.parse()
        .expect("Failed to decode event");

    for prev_version in prev_generation {
        let parent = prev_version.id;

        let state = EntityStateInterface::new(c, event_time, prev_version);
        event.apply(&state);
        for (successor, next_timed_event) in state.get_successors() {
            let new_version = NewVersion {
                ingest_id,
                entity_type,
                entity_id,
                data: successor,
                from_event,
                observed_by: None,
                next_timed_event,
            };

            new_generation.add_successor(parent, new_version);
        }
    }
}

fn observe_generation<EntityT: sim::Entity>(
    new_generation: &mut MergedSuccessors<NewVersion>,
    version_conflicts: &mut Option<Vec<(i32, String)>>,
    versions: Vec<(Version, Vec<Parent>)>,
    entity_raw: &EntityT::Raw,
    perceived_at: DateTime<Utc>
) {
    for (version, parents) in versions {
        let version_id = version.id;
        match observe_entity::<EntityT>(version, entity_raw, perceived_at) {
            Ok(new_version) => {
                let parent_ids = parents.into_iter()
                    .map(|parent| parent.parent)
                    .collect();
                new_generation.add_multi_parent_successor(parent_ids, new_version);

                // Successful application! Don't need to track conflicts any more.
                *version_conflicts = None;
            }
            Err(conflicts) => {
                if let Some(version_conflicts) = version_conflicts {
                    let conflicts = format!("- {}", conflicts.into_iter().map(|c| c.to_string()).join("\n- "));
                    version_conflicts.push((version_id, conflicts));
                }
            }
        }
    }
}

fn observe_entity<EntityT: sim::Entity>(version: Version, entity_raw: &EntityT::Raw, perceived_at: DateTime<Utc>) -> Result<NewVersion, Vec<Conflict>> {
    let mut entity: EntityT = serde_json::from_value(version.data)
        .expect("Couldn't parse stored version data");

    let conflicts = entity.observe(entity_raw);
    if !conflicts.is_empty() {
        return Err(conflicts);
    }

    Ok(NewVersion {
        ingest_id: version.ingest_id,
        entity_type: EntityT::name(),
        entity_id: version.entity_id,
        data: serde_json::to_value(entity)
            .expect("Failed to serialize entity"),
        from_event: version.from_event,
        observed_by: Some(perceived_at),
        next_timed_event: version.next_timed_event,
    })
}

async fn wait_for_feed_ingest(ingest: &mut IngestState, wait_until_time: DateTime<Utc>) {
    ingest.notify_progress.send(wait_until_time)
        .expect("Error communicating with Chronicler ingest");
    // info!("Chron ingest sent {} as requested time", wait_until_time);

    loop {
        let feed_time = *ingest.receive_progress.borrow();
        if wait_until_time < feed_time {
            break;
        }
        // info!("Chronicler ingest waiting for Eventually ingest to catch up (at {} and we need {}, difference of {}s)",
        //     feed_time, wait_until_time, (wait_until_time - feed_time).num_seconds());
        ingest.receive_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}
