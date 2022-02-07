use std::cmp::{min, max, Ordering};
use std::collections::BinaryHeap;
use std::iter;
use std::pin::Pin;
use chrono::{DateTime, Duration, Utc};
use diesel::{self, Connection, insert_into, RunQueryDsl};
use futures::{stream, Stream, StreamExt};
use rocket::{info};
use uuid::Uuid;
use itertools::{Itertools};
use crate::api::{chronicler, ChroniclerItem};
use crate::ingest::task::IngestState;

use crate::sim;
use crate::schema::*;
use crate::state::{StateInterface};


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
    canonical: bool,
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
    canonical: bool,
    data: serde_json::Value,
}

impl InsertChronUpdate {
    fn from_chron(ingest_id: i32, entity_type: &'static str, item: ChroniclerItem, resolved: bool, canonical: bool) -> Self {
        InsertChronUpdate {
            ingest_id,
            entity_type,
            entity_id: item.entity_id,
            perceived_at: item.valid_from,
            earliest_time: item.valid_from - Duration::seconds(15),
            latest_time: item.valid_from + Duration::seconds(15),
            resolved,
            canonical,
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
                    // Initial state is presumed resolved and canonical
                    InsertChronUpdate::from_chron(ingest_id, entity_type, entity, true, true)
                });

            Box::pin(stream) as Pin<Box<dyn Stream<Item=InsertChronUpdate> + Send>>
        })
        .chain(iter::once(
            Box::pin(chronicler::schedule(start_at_time)
                .map(move |entity| {
                    // Initial state is presumed resolved and canonical
                    InsertChronUpdate::from_chron(ingest_id, "game", entity, true, true)
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

struct GameUpdateSortable {
    item: InsertChronUpdate,
    play_count: i64,
}

impl GameUpdateSortable {
    pub fn new(mut item: InsertChronUpdate) -> GameUpdateSortable {
        let last_update_full = item.data.get("lastUpdateFull")
            .expect("Game updates must have lastUpdateFull");
        if let Some(updates) = last_update_full.as_array() {
            if let Some(first_update) = updates.first() {
                let created = first_update.get("created")
                    .expect("lastUpdateFull entry must have a 'created' property");
                let created: DateTime<Utc> = serde_json::from_value(created.clone())
                    .expect("Couldn't deserialize 'created' into a date");

                item.earliest_time = created;
                item.latest_time = created;
            }
        }

        let play_count = item.data.get("playCount")
            .expect("Game update must have play count")
            .as_i64()
            .expect("Play count must be an integer");

        GameUpdateSortable { item, play_count }
    }
}

impl Eq for GameUpdateSortable {}

impl PartialEq<Self> for GameUpdateSortable {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl PartialOrd<Self> for GameUpdateSortable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GameUpdateSortable {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.play_count == other.play_count {
            other.item.perceived_at.cmp(&self.item.perceived_at)
        } else {
            other.play_count.cmp(&self.play_count)
        }
    }
}

fn reorder_game_updates<StreamT: Stream<Item=InsertChronUpdate>>(stream: StreamT) -> impl Stream<Item=InsertChronUpdate> {
    let stream = Box::pin(stream
        .map(GameUpdateSortable::new)
        .peekable());
    stream::unfold((BinaryHeap::new(), stream), move |(mut buf, mut stream)| async move {
        if buf.is_empty() {
            buf.push(stream.as_mut().next().await.expect("Chron stream ended! Not implemented yet"))
        }

        let buffer_until = buf.peek().unwrap().item.latest_time + Duration::minutes(5);
        while let Some(next_item) = stream.as_mut().next_if(
            |item| item.item.latest_time < buffer_until).await {
            buf.push(next_item)
        }

        Some((buf.pop().unwrap().item, (buf, stream)))
    })
}

pub async fn ingest_chron(ingest: IngestState, start_at_time: &'static str) {
    info!("Started Chron ingest task");

    // Have to move ingest in and back out even though that's the whole point of borrows
    let ingest = fetch_initial_state(ingest, start_at_time).await;

    info!("Finished populating initial Chron values");

    let streams = chronicler::ENDPOINT_NAMES.into_iter()
        .map(move |entity_type| {
            let stream = chronicler::versions(entity_type, start_at_time)
                .map(move |entity| {
                    InsertChronUpdate::from_chron(ingest.ingest_id, entity_type, entity, false, false)
                });

            Box::pin(stream) as ChronUpdateStreamPin
        })
        .chain(iter::once(
            Box::pin(reorder_game_updates(
                chronicler::game_updates(start_at_time)
                    .map(move |entity| {
                        InsertChronUpdate::from_chron(ingest.ingest_id, "game", entity, false, false)
                    })
            )) as ChronUpdateStreamPin
        ));

    kmerge_chron_updates(streams)
        .fold(ingest, |mut ingest, update| async {
            // I sort of redefined the semantics of this as chron asking for the feed to be
            // up to date to a given time and now the name isn't a great match
            ingest.notify_progress.send(update.latest_time)
                .expect("Error communicating with Eventually ingest");
            info!("Chron ingest requested Feed updates up to {}", update.latest_time);

            wait_for_feed_ingest(&mut ingest, update.latest_time).await;

            do_ingest(ingest, update).await
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
        if wait_until_time < feed_time {
            break;
        }
        info!("Chronicler ingest waiting for Eventually ingest to catch up (at {} and we need {}, difference of {}s)",
            feed_time, wait_until_time, (wait_until_time - feed_time).num_seconds());
        ingest.receive_progress.changed().await
            .expect("Error communicating with Eventually ingest");
    }
}

async fn do_ingest(ingest: IngestState, mut update: InsertChronUpdate) -> IngestState {
    info!("Starting ingest for {} {} timestamped {}", update.entity_type, update.entity_id, update.perceived_at);
    ingest.db.run(move |c| {
        let state = StateInterface {
            conn: &c,
            ingest_id: ingest.ingest_id,
        };

        // Optimization for game updates: The order in which entity versions are perceived is reliable,
        // so we can fetch the previous update for this item and we know that this update's
        // earliest_time must be no earlier than the previous version's earliest_time. This is only
        // worth doing for game events, because other types of updates are usually so far apart that
        // there cannot be any overlap. There is a time cost to fetching the previous update so it's not
        // worth doing if it's unlikely to be useful.
        if update.entity_type == "game" {
            update.earliest_time = max(
                update.earliest_time,
                state.previous_update_earliest_time(&update.entity_type,
                                                    update.entity_id,
                                                    update.perceived_at),
            );
        }

        state.conn.transaction(|| {
            if let Some((start, maybe_end, is_canonical)) = find_placement(&state, &update) {
                update.earliest_time = max(update.earliest_time, start);
                if let Some(end) = maybe_end {
                    update.latest_time = min(update.latest_time, end);
                }
                update.resolved = true;
                update.canonical = is_canonical;
                info!("Inserting resolved {} update", update.entity_type);
            } else {
                info!("Inserting unresolved {} update", update.entity_type);
            }

            // Copy these out before moving update
            let resolved = update.resolved;
            let entity_type = update.entity_type;
            let entity_id = update.entity_id;
            let perceived_at = update.perceived_at;
            let earliest_time = update.earliest_time;

            insert_into(crate::schema::chron_updates::dsl::chron_updates).values(update).execute(c)?;

            // TODO Isn't there some amount of bounding I can do even if the update isn't resolved?
            if resolved {
                let should_re_resolve = state.bound_previous_update(
                    entity_type, entity_id, perceived_at, earliest_time);
                if let Some(_re_resolve_id) = should_re_resolve {
                    todo!();
                }
            }

            Ok::<_, diesel::result::Error>(())
        })
    }).await.expect("Database error processing ingest");

    ingest
}

fn find_placement(state: &StateInterface, this_update: &InsertChronUpdate) -> Placement {
    match this_update.entity_type {
        "player" => find_placement_typed::<sim::Player>(state, this_update),
        "sim" => find_placement_typed::<sim::Sim>(state, this_update),
        "game" => find_placement_typed::<sim::Game>(state, this_update),
        "standings" => find_placement_typed::<sim::Standings>(state, this_update),
        "team" => find_placement_typed::<sim::Team>(state, this_update),
        other => panic!("Unknown entity type {}", other)
    }
}

type Placement = Option<(DateTime<Utc>, Option<DateTime<Utc>>, bool)>;

fn find_placement_typed<'a, EntityT>(state: &StateInterface, this_update: &InsertChronUpdate) -> Placement
    where EntityT: sim::Entity {
    info!("Trying to place {} {} between {} and {}",
        this_update.entity_type, this_update.entity_id,
        this_update.earliest_time, this_update.latest_time);

    info!("Computing entity state at start of range");
    let mut versions = state.versions::<EntityT>(this_update.entity_id,
                                                 this_update.earliest_time,
                                                 this_update.latest_time);
    let expected_entity = EntityT::new(this_update.data.clone());
    // Before calling next(), current_entity() returns the previous resolved version
    let (starting_conflicts, canonical) = versions.current_entity().get_conflicts(&expected_entity, this_update.earliest_time);
    assert!(canonical, "The starting version for a version iteration must be canonical");

    let mut conflicts = Vec::new();
    let mut valid_versions = Vec::new();
    while let Some(version) = versions.next() {
        let (conflict_str, is_canonical) = version.entity.get_conflicts(&expected_entity, version.valid_from);
        if let Some(conflict_str) = conflict_str {
            conflicts.push((version, conflict_str))
        } else {
            valid_versions.push((version, is_canonical))
        }
    }

    match (valid_versions.len(), conflicts.len()) {
        (0, 0) => {
            if let Some(starting_conflicts) = starting_conflicts {
                panic!("{} update differs from previous value and there are no events to explain why:\n{}",
                       this_update.entity_type, starting_conflicts);
            } else {
                panic!("Expected two consecutive Chron records for {} {} to differ, but they did not",
                       this_update.entity_type, this_update.entity_id);
            }
        }
        (0, _) => {
            let placement_reasons = conflicts.into_iter().map(|(version, conflicts)| {
                format!("Between {:#?} and {:#?}, after event {}:\n{}",
                        version.valid_from,
                        version.valid_until.map(|t| format!("{:?}", t)).unwrap_or("(unknown)".to_string()),
                        version.from_event_debug,
                        conflicts)
            }).join("\n");
            panic!("{} update cannot ever be placed -- no valid placements:\n{}", this_update.entity_type, placement_reasons);
        }
        (1, _) => {
            info!("{} update can be placed", this_update.entity_type);
            let (placed_version, is_canonical) = valid_versions.into_iter().exactly_one().ok().unwrap();
            Some((placed_version.valid_from, placed_version.valid_until, is_canonical))
        }
        (_, _) => {
            info!("{} update cannot currently be placed -- multiple valid placements", this_update.entity_type);
            None
        }
    }
}
