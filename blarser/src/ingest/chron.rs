use std::cmp::{max, Ordering};
use std::collections::BinaryHeap;
use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::{self, Connection, ExpressionMethods, insert_into, PgConnection, RunQueryDsl};
use futures::{stream, Stream, StreamExt};
use rocket::{info};
use uuid::Uuid;
use itertools::{Itertools, Either};
use crate::api::{chronicler, ChroniclerItem, EventuallyEvent};
use crate::ingest::task::IngestState;

use diesel::prelude::*;
use crate::ingest::sim;
use crate::ingest::sim::FeedEventChangeResult;
use crate::schema::*;


#[derive(Queryable, PartialEq, Debug)]
struct ChronUpdate {
    id: i32,
    ingest_id: i32,
    entity_type: String,
    entity_id: Uuid,
    perceived_at: DateTime<Utc>,
    earliest_time: DateTime<Utc>,
    latest_time: DateTime<Utc>,
    resolved: bool,
    data: serde_json::Value,
}

#[derive(Queryable)]
#[allow(dead_code)]
struct FeedEventChange {
    id: i32,
    ingest_id: i32,
    entity_type: String,
    entity_id: Uuid,
    perceived_at: NaiveDateTime,
    earliest_time: NaiveDateTime,
    latest_time: NaiveDateTime,
    resolved: bool,
    data: serde_json::Value,
}

#[derive(Insertable)]
#[table_name = "chron_updates"]
struct InsertChronUpdate {
    ingest_id: i32,
    entity_type: &'static str,
    entity_id: Uuid,
    perceived_at: DateTime<Utc>,
    earliest_time: DateTime<Utc>,
    latest_time: DateTime<Utc>,
    resolved: bool,
    data: serde_json::Value,
}

impl InsertChronUpdate {
    fn from_chron(ingest_id: i32, entity_type: &'static str, item: ChroniclerItem, resolved: bool) -> Self {
        InsertChronUpdate {
            ingest_id,
            entity_type,
            entity_id: item.entity_id,
            perceived_at: item.valid_from,
            earliest_time: item.valid_from - Duration::seconds(5),
            latest_time: item.valid_from + Duration::seconds(5),
            resolved,
            data: item.data,
        }
    }
}

async fn fetch_initial_state(ingest: IngestState, start_at_time: &'static str) -> IngestState {
    let ingest_id = ingest.ingest_id;
    // So much of this is just making the type system happy
    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::entities(entity_type, start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest_id, entity_type, entity, true)
                });

            Box::pin(stream) as Pin<Box<dyn Stream<Item=InsertChronUpdate> + Send>>
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest_id, "game", entity, true)
                })
            ) as Pin<Box<dyn Stream<Item=InsertChronUpdate> + Send>>
        ));

    // There are so many objects that Diesel can't insert them all in one operation
    let inserts_chunked: Vec<_> = stream::select_all(streams)
        .chunks(1000)
        .collect().await;

    ingest.db.run(|c| {
        c.transaction(|| {
            use crate::schema::chron_updates::dsl::*;

            for insert_chunk in inserts_chunked {
                insert_into(chron_updates).values(insert_chunk).execute(c)?;
            }

            Ok::<_, diesel::result::Error>(())
        })
    }).await.expect("Failed to store initial state from chron");

    ingest
}

type ChronUpdateStreamPin = Pin<Box<dyn Stream<Item=InsertChronUpdate> + Send>>;
pub async fn ingest_chron(ingest: IngestState, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    // Have to move ingest in and back out even though that's the whole point of borrows
    let ingest = fetch_initial_state(ingest, start_at_time).await;

    info!("Finished populating initial Chron values");

    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest.ingest_id, entity_type, entity, true)
                });

            Box::pin(stream) as ChronUpdateStreamPin
        })
        .chain(iter::once(
            Box::pin(chronicler::game_updates(start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest.ingest_id, "game", entity, true)
                })
            ) as ChronUpdateStreamPin
        ));

    kmerge_chron_updates(streams)
        .fold(ingest, |mut ingest, update| async {
            wait_for_feed_ingest(&mut ingest, update.latest_time).await;

            let update_time = do_ingest(&mut ingest, update).await;

            ingest.notify_progress.send(update_time)
                .expect("Error communicating with Eventually ingest");

            ingest
        }).await;
}

// TODO this is not compatible with live data. It currently relies on a stream closing when it
//   catches up to live data. There's a github issue for a standard kmerge_by for streams,
//   hopefully it at least has a proposed implementation by the time I get to live data.
fn kmerge_chron_updates<StreamT: Iterator<Item=ChronUpdateStreamPin>>(streams_it: StreamT) -> impl Stream<Item=InsertChronUpdate> {
    struct KmergeData {
        stream: ChronUpdateStreamPin,
        // next_date is the earliest date that the next item might have. if the next item is known,
        // this is that item's date (and next_item is Some). if the next item is not yet available,
        // this is the date of the last completed ingest from Eventually (TODO)
        next_date: DateTime<Utc>,
        next_item: Option<InsertChronUpdate>,
    }
    impl Eq for KmergeData {}
    impl PartialEq<Self> for KmergeData {
        fn eq(&self, other: &Self) -> bool {
            self.next_date.eq(&other.next_date)
        }
    }
    impl PartialOrd<Self> for KmergeData {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            // Swap the order to turn > into < and vice versa -- this turns the BinaryHeap from
            // max-first (the default) to min-first
            other.next_date.partial_cmp(&self.next_date)
        }
    }
    impl Ord for KmergeData {
        fn cmp(&self, other: &Self) -> Ordering {
            // Swap the order to turn > into < and vice versa
            other.next_date.cmp(&self.next_date)
        }
    }

    // Use stream::once to do async initialization without having to make the function async
    stream::once(async {
        let streams_init: BinaryHeap<_> = stream::iter(streams_it)
            // This throws away endpoints with no data -- see note at top of function
            .filter_map(|mut stream| async {
                match stream.next().await {
                    None => None,
                    Some(first_item) => {
                        let date = first_item.perceived_at;
                        Some(KmergeData {
                            stream,
                            next_date: date,
                            next_item: Some(first_item),
                        })
                    }
                }
            })
            .collect().await;
        stream::unfold(streams_init, |mut heap| async {
            let mut heap_entry = heap.pop()
                .expect("This heap should never terminate");
            let item_to_yield = heap_entry.next_item;
            // TODO Also fetch the latest ingest time somehow
            let next_item = heap_entry.stream.next().await;
            heap_entry.next_date = match &next_item {
                Some(item) => item.perceived_at,
                None => todo!(),  // This is where I need to use the last ingest time
            };
            heap_entry.next_item = next_item;
            heap.push(heap_entry);

            Some((item_to_yield, heap))
        })
            .filter_map(|x| async {x})
    }).flatten()
}

async fn wait_for_feed_ingest(ingest: &mut IngestState, wait_until_time: DateTime<Utc>) {
    loop {
        let feed_time = *ingest.receive_progress.borrow();
        if feed_time < wait_until_time {
            break;
        }
        info!("Chronicler ingest waiting for Eventually ingest to catch up ({}s)",
            (wait_until_time - feed_time).num_seconds());
        ingest.receive_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}

async fn do_ingest(ingest: &mut IngestState, mut update: InsertChronUpdate) -> DateTime<Utc> {
    info!("Doing ingest");
    let time = update.latest_time;

    ingest.db.run(move |c| {
        c.transaction(|| {
            let prev = get_previous_resolved_update(c, &update);

            // We know for sure that no other update can overlap with `prev` because it's resolved.
            // Therefore, we know this update's earliest_time should be no earlier than `prev`'s
            // latest_time
            update.earliest_time = max(update.earliest_time, prev.latest_time);

            let feed_events = get_possible_feed_events(c, &update, prev.latest_time);

            find_placement(update, prev, feed_events);
            Ok::<_, diesel::result::Error>(())
        })
    }).await.expect("Database error processing ingest");

    time
}

fn find_placement<FeedIterT>(this_update: InsertChronUpdate, prev_update: ChronUpdate, feed_events: FeedIterT)
    where FeedIterT: Iterator<Item=EventuallyEvent> {
    match this_update.entity_type {
        "player" => find_placement_typed::<sim::Player, _>(this_update, prev_update, feed_events),
        "sim" => find_placement_typed::<sim::Sim, _>(this_update, prev_update, feed_events),
        other => panic!("Unknown entity type {}", other)
    }
}

fn to_bulleted_list(vec: Vec<String>) -> Option<String> {
    if vec.is_empty() {
        return None;
    }

    Some(format!("- {}", vec.join("\n- ")))
}

fn find_placement_typed<'a, EntityT, FeedIterT>(this_update: InsertChronUpdate, prev_update: ChronUpdate, feed_events: FeedIterT)
    where EntityT: sim::Entity, FeedIterT: Iterator<Item=EventuallyEvent> {
    let expected_entity = EntityT::new(this_update.data);
    let mut entity = EntityT::new(prev_update.data);
    let starting_conflicts = entity.get_conflicts(&expected_entity);
    let (oks, fails): (Vec<_>, Vec<_>) = feed_events
        .flat_map(|event| {
            match entity.apply_feed_event(&event) {
                FeedEventChangeResult::DidNotApply => {
                    info!("{:?} event did not apply", event.r#type);
                    None
                }
                FeedEventChangeResult::Incompatible(_error) => todo!(),
                FeedEventChangeResult::Ok => {
                    match to_bulleted_list(entity.get_conflicts(&expected_entity)) {
                        None => {
                            info!("Change could be placed after {:?} event", event.r#type);
                            Some(Ok(event.created))
                        }
                        Some(conflicts) => {
                            info!("Change could not placed after {:?} event:\n{}", event.r#type, conflicts);
                            Some(Err(conflicts))
                        }
                    }
                }
            }
        })
        .partition_map(|r| {
            match r {
                Ok(v) => Either::Left(v),
                Err(v) => Either::Right(v),
            }
        });
    match (oks.len(), fails.len()) {
        (0, 0) => {
            match to_bulleted_list(starting_conflicts) {
                None => {
                    panic!("Expected two consecutive Chron records for {} {} to differ, but they did not", this_update.entity_type, this_update.entity_id);
                }
                Some(conflicts) => {
                    panic!("{} update differs from previous value and there are no feed events to explain why:\n{}", this_update.entity_type, conflicts);
                }
            }
        }
        (0, _) => {
            panic!("{} update cannot ever be placed -- no valid placements", this_update.entity_type);
        }
        (1, _) => {
            info!("{} update can be placed", this_update.entity_type);
            todo!()
        }
        (_, _) => {
            info!("{} update cannot currently be placed -- multiple valid placements", this_update.entity_type);
            todo!()
        }
    }
}

fn get_previous_resolved_update(c: &PgConnection, update: &InsertChronUpdate) -> ChronUpdate {
    use crate::schema::chron_updates::dsl::*;
    chron_updates
        .filter(ingest_id.eq(update.ingest_id))
        .filter(entity_type.eq(update.entity_type))
        .filter(entity_id.eq(update.entity_id))
        .filter(resolved.eq(true))
        .filter(latest_time.lt(update.latest_time))
        .limit(1)
        .load::<ChronUpdate>(c)
        .expect("Error querying previous record for entity type")
        .into_iter().next()
        .expect("Couldn't find a previous record for entity type")
}

fn get_possible_feed_events(c: &PgConnection, update: &InsertChronUpdate, after_time: DateTime<Utc>) -> impl Iterator<Item=EventuallyEvent> {
    use crate::schema::feed_events::dsl as feed;
    use crate::schema::feed_event_changes::dsl as changes;
    changes::feed_event_changes
        .inner_join(feed::feed_events)
        .select((feed::data, ))
        .filter(feed::ingest_id.eq(update.ingest_id))
        .filter(feed::created_at.ge(after_time))
        .filter(feed::created_at.lt(update.latest_time))
        .filter(changes::entity_type.eq(update.entity_type))
        .filter(changes::entity_id.eq(update.entity_id).or(changes::entity_id.is_null()))
        .load::<(serde_json::Value, )>(c)
        .expect("Error querying feed events that change this entity")
        .into_iter()
        .map(|json| {
            serde_json::from_value(json.0)
                .expect("Couldn't parse stored Feed event")
        })
}