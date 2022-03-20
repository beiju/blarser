use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap};
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
use partial_information::{Conflict};
use async_trait::async_trait;

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::ChronIngest;
use crate::{sim, EntityStateInterface};
use crate::ingest::approvals_db::{ApprovalState, get_approval};
use crate::state::{Event, MergedSuccessors, add_initial_versions, terminate_versions, get_entity_update_tree, Version, Parent, NewVersion, save_versions};
use crate::sim::{entity_dispatch};

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

#[async_trait]
trait Observation {
    fn sort_time(&self) -> DateTime<Utc>;

    async fn do_ingest(self: Box<Self>, ingest: &mut ChronIngest);
}

impl Eq for dyn Observation + Send {}

impl PartialEq<Self> for dyn Observation + Send {
    fn eq(&self, other: &Self) -> bool {
        self.sort_time().eq(&other.sort_time())
    }
}

impl PartialOrd<Self> for dyn Observation + Send {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.sort_time().partial_cmp(&other.sort_time())
    }
}

impl Ord for dyn Observation + Send {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_time().cmp(&other.sort_time())
    }
}

type BoxedObservation = Box<dyn Observation + Send>;

struct EntityObservation<EntityT: sim::Entity> {
    entity_id: Uuid,
    entity_raw: EntityT::Raw,
    perceived_at: DateTime<Utc>,
    earliest_time: DateTime<Utc>,
    latest_time: DateTime<Utc>,
}

#[async_trait]
impl<EntityT: 'static + sim::Entity> Observation for EntityObservation<EntityT> {
    fn sort_time(&self) -> DateTime<Utc> {
        self.latest_time
    }

    async fn do_ingest(self: Box<Self>, ingest: &mut ChronIngest) {
        wait_for_feed_ingest(ingest, self.latest_time).await;

        let ingest_id = ingest.ingest_id;
        let (approval, this) = ingest.db.run(move |c| {
            let approval_result = c.build_transaction()
                .serializable()
                .run(|| {
                    let conflicts = self.do_ingest_internal(c, ingest_id, false);

                    // Round-trip through the Result machinery to get diesel to cancel the transaction
                    match conflicts {
                        None => { Ok(()) }
                        Some(c) => { Err(IngestError::NeedsApproval(c)) }
                    }
                });

            if let Err(IngestError::NeedsApproval(approval)) = approval_result {
                (Some(approval), self)
            } else {
                approval_result.expect("Unexpected database error in chronicler ingest");
                (None, self)
            }
        }).await;

        if let Some(conflicts) = approval {
            // TODO Make a fun html debug view from conflicts info
            let message = conflicts.into_iter()
                .map(|(_, reason)| {
                    // TODO Print the info about which version the conflict is from, not about which
                    //   observation we tried to apply
                    format!("Can't apply observation to {} {} at {}:\n{}",
                            EntityT::name(), this.entity_id, this.perceived_at, reason)
                })
                .join("\n");

            let entity_id = this.entity_id;
            let entity_time = this.perceived_at;
            let approval = ingest.db.run(move |c| {
                get_approval(c, EntityT::name(), entity_id, entity_time, &message)
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
                        let conflicts = this.do_ingest_internal(c, ingest_id, true);

                        assert!(conflicts.is_none(), "Generated conflicts even with force=true");
                        Ok::<_, diesel::result::Error>(())
                    })
                }).await.unwrap();
            } else {
                panic!("Approval rejected")
            }
        }
    }
}

impl<EntityT: sim::Entity> EntityObservation<EntityT> {
    fn do_ingest_internal(&self, c: &PgConnection, ingest_id: i32, force: bool) -> Option<Vec<(i32, String)>> {
        info!("Placing {} {} between {} and {}", EntityT::name(), self.entity_id, self.earliest_time, self.latest_time);

        let (events, generations) = get_entity_update_tree(c, ingest_id, EntityT::name(), self.entity_id, self.earliest_time)
            .expect("Error getting events for Chronicler ingest");

        if self.entity_id.to_string() == "781feeac-f948-43af-beee-14fa1328db76" && self.earliest_time.to_string() == "2021-12-06 16:00:10.303 UTC" {
            info!("BREAK");
        }

        let mut to_terminate = None;

        let mut prev_generation = Vec::new();
        let mut version_conflicts = Some(Vec::new());
        for (event, versions) in events.into_iter().zip(generations) {
            let mut new_generation = MergedSuccessors::new();

            if event.event_time <= self.latest_time {
                to_terminate = Some(versions.iter().map(|(v, _)| v.id).collect());
                observe_generation::<EntityT>(&mut new_generation, &mut version_conflicts, versions, &self.entity_raw, self.perceived_at, force);
            }

            advance_generation(c, ingest_id, &mut new_generation, EntityT::name(), self.entity_id, event, prev_generation);

            prev_generation = save_versions(c, new_generation.into_inner())
                .expect("Error saving updated versions");
        }

        if let Some(to_terminate) = to_terminate {
            terminate_versions(c, to_terminate, format!("Failed to apply observation at {}", self.perceived_at))
                .expect("Failed to terminate versions");
        }

        if version_conflicts.is_some() {
            info!("Conflicts!");
        }

        version_conflicts
    }
}


fn new_observation<EntityT: 'static + sim::Entity>(item: ChroniclerItem) -> BoxedObservation {
    let entity_raw = serde_json::from_value(item.data)
        .expect("Error deserializing raw entity data from Chronicler");

    let (earliest_time, latest_time) = EntityT::time_range_for_update(item.valid_from, &entity_raw);

    let obs = EntityObservation::<EntityT> {
        entity_id: item.entity_id,
        entity_raw,
        perceived_at: item.valid_from,
        earliest_time,
        latest_time,
    };

    Box::new(obs)
}

type PinnedObservationStream = Pin<Box<dyn Stream<Item=BoxedObservation> + Send>>;

fn chron_updates(start_at_time: &'static str) -> impl Stream<Item=BoxedObservation> {
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                // Using a filter_map so I can ignore entity types I'm not parsing yet (it's a lot
                // of work to even parse them correctly). The only reason the block is async is that
                // the API of filter_map requires it.
                .filter_map(move |item| async move {
                    let observation = entity_dispatch!(entity_type => new_observation(item);
                                       _ => return None);

                    Some(observation)
                });

            Box::pin(stream) as PinnedObservationStream
        })
        .chain(iter::once({
            let stream = chronicler::game_updates(start_at_time)
                .map(new_observation::<sim::Game>);

            Box::pin(stream) as PinnedObservationStream
        }));

    kmerge_stream(streams)
}

fn kmerge_stream(streams: impl Iterator<Item=PinnedObservationStream>) -> impl Stream<Item=BoxedObservation> {
    let peekable_streams: Vec<_> = streams
        .map(|s| (
            s.fuse(),
            BinaryHeap::with_capacity(100)
        ))
        .collect();

    stream::unfold(peekable_streams, |mut streams| async {
        // Refill caches
        for (stream, cache) in &mut streams {
            while cache.len() < 100 {
                if let Some(next) = stream.next().await {
                    cache.push(Reverse(next));
                } else {
                    break; // Avoids infinite loop
                }
            }
        }

        let selected_stream = *streams.iter()
            .enumerate()
            .filter_map(|(i, (_, cache))| {
                cache.peek().map(|Reverse(v)| (i, v))
            })
            .collect::<Vec<_>>()
            .iter()
            .min_by_key(|(_, date)| date)
            .map(|(i, _)| i)
            .expect("TODO: Handle end of all streams");

        let (_, cache) = &mut streams[selected_stream];
        let Reverse(next) = cache.pop()
            .expect("selected_stream should never refer to a stream that doesn't have a next element");

        Some((next, streams))
    })
}

pub async fn init_chron(ingest: &mut ChronIngest, start_at_time: &'static str, start_time_parsed: DateTime<Utc>) {
    let initial_versions: Vec<_> = initial_state(start_at_time).collect().await;
    add_initial_versions(&ingest.db, ingest.ingest_id, start_time_parsed, initial_versions).await;

    info!("Finished populating initial Chron values");
}

pub async fn ingest_chron(mut ingest: ChronIngest, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    let updates = chron_updates(start_at_time);

    pin_mut!(updates);

    while let Some(observation) = updates.next().await {
        observation.do_ingest(&mut ingest).await;
    }
}

#[derive(Debug, Error)]
enum IngestError {
    #[error("Needs approval: {0:?}")]
    NeedsApproval(Vec<(i32, String)>),

    #[error(transparent)]
    DieselError(#[from] diesel::result::Error),
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
                observations: Vec::new(),
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
    perceived_at: DateTime<Utc>,
    force: bool,
) {
    for (version, parents) in versions {
        let version_id = version.id;
        match observe_entity::<EntityT>(version, entity_raw, perceived_at, force) {
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

fn observe_entity<EntityT: sim::Entity>(version: Version, entity_raw: &EntityT::Raw, perceived_at: DateTime<Utc>, force: bool) -> Result<NewVersion, Vec<Conflict>> {
    let mut entity: EntityT = serde_json::from_value(version.data)
        .expect("Couldn't parse stored version data");

    if force {
        entity = EntityT::from_raw(entity_raw.clone());
    } else {
        let conflicts = entity.observe(entity_raw);
        if !conflicts.is_empty() {
            return Err(conflicts);
        }
    }

    let mut observations = version.observations;
    observations.push(perceived_at);

    Ok(NewVersion {
        ingest_id: version.ingest_id,
        entity_type: EntityT::name(),
        entity_id: version.entity_id,
        data: serde_json::to_value(entity)
            .expect("Failed to serialize entity"),
        from_event: version.from_event,
        observations,
        next_timed_event: version.next_timed_event,
    })
}

async fn wait_for_feed_ingest(ingest: &mut ChronIngest, wait_until_time: DateTime<Utc>) {
    ingest.send_chron_progress.send(wait_until_time)
        .expect("Error communicating with Chronicler ingest");
    info!("Chron ingest sent {} as requested time", wait_until_time);

    loop {
        let feed_time = *ingest.receive_feed_progress.borrow();
        if wait_until_time < feed_time {
            break;
        }
        info!("Chronicler ingest waiting for Eventually ingest to catch up (at {} and we need {}, difference of {}s)",
            feed_time, wait_until_time, (wait_until_time - feed_time).num_seconds());
        ingest.receive_feed_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}
