use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Utc};
use futures::{pin_mut, stream, Stream, StreamExt};
use rocket::info;

use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::ChronIngest;
use crate::{entity};
use crate::entity::EntityParseError;
use crate::ingest::observation::Observation;
use crate::state::add_initial_versions;

fn initial_state(start_at_time: &'static str) -> impl Stream<Item=Observation> {
    type ObservationStream = Pin<Box<dyn Stream<Item=Observation> + Send>>;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::entities(entity_type, start_at_time)
                // The whole purpose of the filter_map is to silently ignore UnknownEntity errors,
                // because it's a pain to write the data structure to properly deserialize a whole
                // entity type and I want to defer it until I actually implement the entity.
                // It's async because the signature of filter_map requires it
                .filter_map(move |item| async {
                    match Observation::from_chron(entity_type, item) {
                        Err(EntityParseError::UnknownEntity(_)) => None,
                        other => Some(other.unwrap()),
                    }
                });

            Box::pin(stream) as ObservationStream
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |item| Observation::from_chron("game", item).unwrap())
            ) as ObservationStream
        ));

    stream::select_all(streams)
}

type PinnedObservationStream = Pin<Box<dyn Stream<Item=Observation> + Send>>;

fn chron_updates(start_at_time: &'static str) -> impl Stream<Item=Observation> {
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                // See note on equivalent function in initial_state
                .filter_map(move |item| async {
                    match Observation::from_chron(entity_type, item) {
                        Err(EntityParseError::UnknownEntity(_)) => None,
                        other => Some(other.unwrap()),
                    }
                });

            Box::pin(stream) as PinnedObservationStream
        })
        .chain(iter::once({
            let stream = chronicler::game_updates(start_at_time)
                .map(|item| Observation::from_chron("game", item).unwrap());

            Box::pin(stream) as PinnedObservationStream
        }));

    kmerge_stream(streams)
}

fn kmerge_stream(streams: impl Iterator<Item=PinnedObservationStream>) -> impl Stream<Item=Observation> {
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