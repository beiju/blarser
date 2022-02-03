use std::cmp::{min, max, Ordering};
use std::collections::BinaryHeap;
use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::{self, Connection, ExpressionMethods, insert_into, PgConnection, RunQueryDsl};
use futures::{stream, Stream, StreamExt};
use rocket::{info, State};
use uuid::Uuid;
use itertools::{Itertools};
use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;

use crate::sim::{self, FeedEventChangeResult};
use crate::schema::*;
use crate::state::StateInterface;


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
            earliest_time: item.valid_from - Duration::seconds(15),
            latest_time: item.valid_from + Duration::seconds(15),
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

            let (update_time, ingest) = do_ingest(ingest, update).await;

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
            .filter_map(|x| async { x })
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

async fn do_ingest(ingest: IngestState, mut update: InsertChronUpdate) -> (DateTime<Utc>, IngestState) {
    info!("Starting ingest for {} {} timestamped {}", update.entity_type, update.entity_id, update.perceived_at);
    let time = update.latest_time;

    ingest.db.run(move |c| {
        let state = StateInterface {
            conn: &c,
            ingest_id: ingest.ingest_id
        };

        state.conn.transaction(|| {
            if let Some(placement) = find_placement(&state, &update) {
                // TODO Do I need to do min and max? Is span_start and span_end sufficient?
                update.earliest_time = max(update.earliest_time, placement.span_start);
                update.latest_time = min(update.latest_time, placement.span_end);
                update.resolved = true;
                info!("Inserting resolved {} update", update.entity_type);
            } else {
                info!("Inserting unresolved {} update", update.entity_type);
            }

            insert_into(crate::schema::chron_updates::dsl::chron_updates).values(update).execute(c)

            // TODO: If the previous update's span overlaps this update's span, shrink it. Then, if
            //  the previous update is unresolved, try resolving it again.
        })
    }).await.expect("Database error processing ingest");

    (time, ingest)
}

fn find_placement(state: &StateInterface, this_update: &InsertChronUpdate) -> Option<Placement> {
    match this_update.entity_type {
        "player" => find_placement_typed::<sim::Player>(state, this_update),
        "sim" => find_placement_typed::<sim::Sim>(state, this_update),
        "game" => find_placement_typed::<sim::Game>(state, this_update),
        other => panic!("Unknown entity type {}", other)
    }
}

fn to_bulleted_list(vec: Vec<String>) -> Option<String> {
    if vec.is_empty() {
        return None;
    }

    Some(format!("- {}", vec.join("\n- ")))
}

struct Placement {
    span_start: DateTime<Utc>,
    span_end: DateTime<Utc>,
    conflicts: Vec<String>,
    after_event: String,
}

fn find_placement_typed<'a, EntityT>(state: &StateInterface, this_update: &InsertChronUpdate) -> Option<Placement>
    where EntityT: sim::Entity {
    info!("Trying to place {} {} between {} and {}",
        this_update.entity_type, this_update.entity_id,
        this_update.earliest_time, this_update.latest_time);

    info!("Computing entity state at start of range");
    let mut entity: EntityT = state.entity(this_update.entity_id, this_update.earliest_time);
    let expected_entity = EntityT::new(this_update.data.clone());
    let starting_conflicts = entity.get_conflicts(&expected_entity);

    let events = state.events_for_entity(this_update.entity_type, this_update.entity_id, this_update.earliest_time, this_update.latest_time);
    // Each event creates a span, starting at the event time and ending at the next event time (or
    // this update's latest time, for the last event)
    let span_ends = events.iter().skip(1).map(|event| event.time).chain(iter::once(this_update.latest_time));

    info!("There are {} potential spans for {} update", events.len(), this_update.entity_type);

    let (oks, fails): (Vec<_>, Vec<_>) = events.iter().zip(span_ends)
        .scan(this_update.earliest_time, |span_start, (event, span_end)| {
            info!("Applying {:?} event to {} {}", event.event_type, this_update.entity_type, this_update.entity_id);
            match entity.apply_event(&event, state) {
                FeedEventChangeResult::DidNotApply => {
                    info!("{:?} event did not apply", event.event_type);
                    None
                }
                FeedEventChangeResult::Ok => {
                    let placement = Placement {
                        span_start: *span_start,
                        span_end,
                        conflicts: entity.get_conflicts(&expected_entity),
                        after_event: format!("{:?}", event.event_type),
                    };
                    *span_start = span_end;
                    Some(placement)
                }
            }
        })
        .partition(|placement| placement.conflicts.is_empty());
    match (oks.len(), fails.len()) {
        (0, 0) => {
            match to_bulleted_list(starting_conflicts) {
                None => {
                    panic!("Expected two consecutive Chron records for {} {} to differ, but they did not", this_update.entity_type, this_update.entity_id);
                }
                Some(conflicts) => {
                    panic!("{} update differs from previous value and there are no events to explain why:\n{}", this_update.entity_type, conflicts);
                }
            }
        }
        (0, _) => {
            let placement_reasons = fails.into_iter().map(|placement| {
                format!("Between {:#?} and {:#?}, after event {}:\n{}",
                        placement.span_start, placement.span_end, placement.after_event,
                        to_bulleted_list(placement.conflicts).unwrap_or("".to_string()))
            }).join("\n");
            panic!("{} update cannot ever be placed -- no valid placements:\n{}", this_update.entity_type, placement_reasons);
        }
        (1, _) => {
            info!("{} update can be placed", this_update.entity_type);
            Some(oks.into_iter().exactly_one().ok().unwrap())
        }
        (_, _) => {
            info!("{} update cannot currently be placed -- multiple valid placements", this_update.entity_type);
            None
        }
    }
}
