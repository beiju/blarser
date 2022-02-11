use std::fmt::{Debug, Formatter};
use std::iter::Peekable;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use diesel::{prelude::*, PgConnection, QueryDsl, insert_into};
use diesel::dsl::max;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use rocket::{info, warn};

use crate::sim::{Entity, FeedEventChangeResult, Sim};
use crate::api::{ChroniclerItem, EventuallyEvent};
use crate::state::{GenericEvent, GenericEventType};
use crate::state::versions_db::NewVersion;

pub struct StateInterface<'conn> {
    pub conn: &'conn &'conn mut PgConnection,
    pub ingest_id: i32,

    // TODO: Cache parameters
}
#[derive(Debug)]
pub struct EntityVersion<EntityT: Entity> {
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub entity: EntityT,
    pub from_event_debug: String,
}

pub struct VersionsIter<'conn, 'state, EntityT: Entity> {
    state: &'state StateInterface<'conn>,
    current_version: EntityT,
    current_version_valid_from: DateTime<Utc>,
    last_applied_event_debug: String,

    updates: Peekable<Box<dyn Iterator<Item=(DateTime<Utc>, EntityT)> + 'state>>,
    feed_events: Peekable<Box<dyn Iterator<Item=EventuallyEvent> + 'state>>,
    start_time: DateTime<Utc>,
    stop_time: DateTime<Utc>,

    stop: bool,

    // Only for debug printouts
    entity_id: Uuid,
}

impl<'conn, 'state, EntityT: Entity> Debug for VersionsIter<'conn, 'state, EntityT> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("VersionsIter")
    }
}


impl<'conn, 'state, EntityT: Entity> Iterator for VersionsIter<'conn, 'state, EntityT> {
    type Item = EntityVersion<EntityT>;

    fn next(&mut self) -> Option<Self::Item> {
        info!("Getting next version for {} {}", EntityT::name(), self.entity_id);
        let mut process_time = self.current_version_valid_from;
        while !self.stop {
            let next_update_time = self.updates.peek().map(|(time, _)| *time);
            let next_feed_event_time = self.feed_events.peek().map(|event| event.created);
            let next_timed_event = self.current_version.next_timed_event(process_time, self.stop_time, self.state);

            if let Some(update_time) = next_update_time {
                let before_feed_event = next_feed_event_time.as_ref().map(|t| update_time < *t).unwrap_or(true);
                let before_timed_event = next_timed_event.as_ref().map(|event| update_time < event.time).unwrap_or(true);

                if before_feed_event && before_timed_event {
                    info!("Replacing computed version with observed version");
                    // Got an update to the current version. Replace stored entity with it
                    let (_, update) = self.updates.next().unwrap();
                    self.current_version = update;
                }
            }

            let next_event = if let Some(feed_event_time) = next_feed_event_time && next_timed_event.as_ref().map(|event| feed_event_time < event.time).unwrap_or(true) {
                let event = self.feed_events.next().unwrap();
                GenericEvent {
                    time: event.created,
                    event_type: GenericEventType::FeedEvent(event),
                }
            } else if let Some(event) = next_timed_event {
                event
            } else {
                // There's no more events!
                self.stop = true;
                info!("Yielding last version for {} {}, valid from {}",
                    EntityT::name(), self.entity_id, self.current_version_valid_from);
                return Some(EntityVersion {
                    valid_from: self.current_version_valid_from,
                    valid_until: None,
                    entity: self.current_version.clone(),
                    from_event_debug: std::mem::take(&mut self.last_applied_event_debug),
                });
            };

            // Version represents the previous version, so need to clone the entity before applying
            // the event
            // must be >, not >=, because a version becomes invalid exactly at its end time
            let version = if next_event.time > self.start_time {
                Some(EntityVersion {
                    valid_from: self.current_version_valid_from,
                    valid_until: Some(next_event.time),
                    entity: self.current_version.clone(),
                    from_event_debug: std::mem::take(&mut self.last_applied_event_debug),
                })
            } else {
                None
            };

            info!("Applying {:?} event", next_event);
            match self.current_version.apply_event(&next_event, self.state) {
                FeedEventChangeResult::DidNotApply => {
                    info!("Event did not apply; continuing");
                    // Just advance process time
                    process_time = next_event.time;
                }
                FeedEventChangeResult::Ok => {
                    self.last_applied_event_debug = format!("{:?}", next_event);
                    self.current_version_valid_from = next_event.time;
                    process_time = next_event.time;
                    if let Some(ref print_version) = version {
                        info!("Yielding new version for {} {} from before this event, valid from {} to {}",
                                EntityT::name(), self.entity_id, print_version.valid_from,
                                print_version.valid_until.unwrap());
                        // Yield and advance time
                        return version;
                    } else {
                        info!("Not yielding version for {} {} that ends at {} because it's before the requested start time",
                                EntityT::name(), self.entity_id, next_event.time);
                    }
                }
            }
        }

        None
    }
}

impl<'conn, 'state, EntityT: Entity> VersionsIter<'conn, 'state, EntityT> {
    pub fn current_entity(&self) -> &EntityT {
        &self.current_version
    }
}

impl<'conn> StateInterface<'conn> {
    pub fn new(c: &'conn &'conn mut PgConnection, ingest_id: i32) -> StateInterface<'conn> {
        StateInterface {
            conn: c,
            ingest_id
        }
    }

    pub fn latest_ingest(c: &'conn &'conn mut PgConnection) -> Option<StateInterface<'conn>> {
        use crate::schema::ingests::dsl;
        dsl::ingests.select(max(dsl::id)).get_result::<Option<i32>>(*c)
            .expect("Query to get latest ID failed")
            .map(move |latest_id| {
                StateInterface {
                    ingest_id: latest_id,
                    conn: c,
                }
            })
    }

    // Inclusive of start time, exclusive of end time
    pub fn versions<'state, EntityT: Entity + 'state>(&'state self, entity_id: Uuid, start_time: DateTime<Utc>, end_time: DateTime<Utc>)
                                                      -> VersionsIter<'conn, 'state, EntityT> {
        info!("Getting {} {} between {} and {}", EntityT::name(), entity_id, start_time, end_time);
        let (entity, entity_start_time): (EntityT, _) = self.last_canonical_entity(entity_id, start_time);
        info!("Most recent canonical entity is at {}", entity_start_time);
        let updates = (
            Box::new(self.version_updates(entity_id, entity_start_time, end_time))
                as Box<(dyn Iterator<Item=(chrono::DateTime<Utc>, EntityT)> + 'state)>
        ).peekable();
        let feed_events = (
            Box::new(self.feed_events_for_entity(EntityT::name(), entity_id, entity_start_time, end_time).into_iter())
                as Box<(dyn Iterator<Item=EventuallyEvent> + 'state)>
        ).peekable();

        VersionsIter {
            state: self,
            current_version: entity,
            current_version_valid_from: entity_start_time,
            last_applied_event_debug: "(unknown)".to_string(),
            updates,
            feed_events,
            start_time,
            stop_time: end_time,
            stop: false,
            entity_id,
        }
    }

    pub fn get_sim(&self, at_time: DateTime<Utc>) -> Sim {
        // TODO Add caching
        self.entity(Uuid::nil(), at_time)
    }

    fn version_updates<EntityT: Entity>(&self, entity_id: Uuid, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> impl Iterator<Item=(DateTime<Utc>, EntityT)> {
        use crate::schema::chron_updates::dsl;
        dsl::chron_updates
            .select((dsl::earliest_time, dsl::data))
            .filter(dsl::ingest_id.eq(self.ingest_id))
            .filter(dsl::entity_type.eq(EntityT::name()))
            .filter(dsl::entity_id.eq(entity_id))
            .filter(dsl::canonical.eq(true))
            .filter(dsl::earliest_time.lt(end_time))
            // ge is important, because sometimes start_time comes from the very object that needs to be
            // returned from this function (that case could be optimized, but it's not worth the effort
            // at the time I'm writing this)
            .filter(dsl::latest_time.ge(start_time))
            // Resolved updates can't overlap, so using either time field should be equivalent
            .order(dsl::latest_time)
            .load::<(DateTime<Utc>, serde_json::Value)>(*self.conn)
            .expect("Error querying versions of entity")
            .into_iter()
            .map(|(t, v)| {
                let entity: EntityT = serde_json::from_value(v)
                    .expect("Error converting stored JSON into entity");
                (t, entity)
            })
    }

    pub fn entity<EntityT: Entity>(&self, entity_id: Uuid, at_time: DateTime<Utc>) -> EntityT {
        info!("Getting {} {} at {}", EntityT::name(), entity_id, at_time);
        let version = self.versions(entity_id, at_time, at_time).exactly_one()
            .expect("Expected exactly one version but got zero or multiple");

        info!("{}, {}", version.valid_from, at_time);
        assert!(version.valid_from <= at_time,
                "Returned version does not become valid until after the requested time");

        assert!(version.valid_until.map(|valid_until| at_time < valid_until).unwrap_or(true),
                "Returned version is no longer valid at the requested time");

        version.entity
    }

    // Inclusive of start time, exclusive of end time
    fn last_canonical_entity<EntityT: Entity>(&self, entity_id: Uuid, at_or_before: DateTime<Utc>) -> (EntityT, DateTime<Utc>) {
        use crate::schema::chron_updates::dsl;

        let last_known = dsl::chron_updates
            .select((dsl::data, dsl::latest_time))
            .filter(dsl::ingest_id.eq(self.ingest_id))
            .filter(dsl::entity_type.eq(EntityT::name()))
            .filter(dsl::entity_id.eq(entity_id))
            .filter(dsl::resolved.eq(true))
            .filter(dsl::canonical.eq(true))
            // This has to be le (not lt) and has to be latest_time
            .filter(dsl::latest_time.le(at_or_before))
            .order(dsl::latest_time.desc())
            .limit(1)
            .load::<(serde_json::Value, DateTime<Utc>)>(*self.conn)
            .expect("Error querying last known version of entity")
            .into_iter().next();

        let (json, time) = match last_known {
            Some((json, time)) => (json, time),
            None => {
                warn!("Didn't find any version of entity after {}, falling back to first version", at_or_before);
                // Fall back to the first version of the entity, and lie about its time
                let (json, resolved, canonical) = dsl::chron_updates
                    .select((dsl::data, dsl::resolved, dsl::canonical))
                    .filter(dsl::ingest_id.eq(self.ingest_id))
                    .filter(dsl::entity_type.eq(EntityT::name()))
                    .filter(dsl::entity_id.eq(entity_id))
                    .order(dsl::earliest_time.asc())
                    .limit(1)
                    .load::<(serde_json::Value, bool, bool)>(*self.conn)
                    .expect("Error querying first known version of entity")
                    .into_iter()
                    .exactly_one()
                    .expect("Couldn't get first known version of entity");

                assert!(resolved, "Fallback version must be resolved");
                assert!(canonical, "Fallback version must be canonical");

                (json, at_or_before)
            }
        };

        let entity = serde_json::from_value(json)
            .expect("Error converting stored JSON into entity");

        (entity, time)
    }

    pub fn previous_update_earliest_time(&self, entity_type: &str, entity_id: Uuid, perceived_before: DateTime<Utc>) -> DateTime<Utc> {
        use crate::schema::chron_updates::dsl;
        dsl::chron_updates
            .select(dsl::earliest_time)
            .filter(dsl::ingest_id.eq(self.ingest_id))
            .filter(dsl::entity_type.eq(entity_type))
            .filter(dsl::entity_id.eq(entity_id))
            .filter(dsl::perceived_at.le(perceived_before))
            .order(dsl::latest_time.desc())
            .limit(1)
            .load::<DateTime<Utc>>(*self.conn)
            .expect("Error querying last known version of entity")
            .into_iter()
            .exactly_one()
            .expect("Expected exactly one response from query")
    }

    pub fn bound_previous_update(&self, entity_type: &str, entity_id: Uuid, perceived_before: DateTime<Utc>, new_latest_time: DateTime<Utc>) -> Option<i32> {
        use crate::schema::chron_updates::dsl;

        // TODO Surely there should be a way to do this in one SQL query
        let target = dsl::chron_updates
            .select(dsl::id)
            .filter(dsl::ingest_id.eq(self.ingest_id))
            .filter(dsl::entity_type.eq(entity_type))
            .filter(dsl::entity_id.eq(entity_id))
            .filter(dsl::perceived_at.le(perceived_before))
            // Only do the update if the new latest time is more restrictive
            .filter(dsl::latest_time.gt(new_latest_time))
            // Only update the latest row that fulfills the other filters
            .order(dsl::perceived_at.desc())
            .limit(1)
            .get_result::<i32>(*self.conn)
            .optional()
            .expect("Error finding previous update");

        if let Some(update_id) = target {
            let is_resolved = diesel::update(dsl::chron_updates)
                .filter(dsl::id.eq(update_id))
                .set(dsl::latest_time.eq(new_latest_time))
                .returning(dsl::resolved)
                .get_result::<bool>(*self.conn)
                .expect("Error updating bound of previous update");

            info!("Updated bound for previous update");
            if is_resolved {
                None
            } else {
                Some(update_id)
            }
        } else {
            info!("Bound for previous update did not overlap this update");
            // If we didn't update any times, we shouldn't try to re-resolve previous updates
            None
        }
    }

    pub fn feed_events_for_entity(&self, entity_type: &str, entity_id: Uuid, from_time: DateTime<Utc>, to_time: DateTime<Utc>) -> Vec<EventuallyEvent> {
        use crate::schema::feed_events::dsl as feed;
        use crate::schema::feed_event_changes::dsl as changes;
        changes::feed_event_changes
            .inner_join(feed::feed_events)
            .select(feed::data)
            .filter(feed::ingest_id.eq(self.ingest_id))
            // Needs to *exclude* events at from_time, because those will already be applied to an
            // entity that's valid_from this from_time, and *include* events at to_time for the
            // analogous reason. This is sort of the opposite of how updates work, so it is
            // confusing. It's probably some sort of fencepost problem.
            .filter(feed::created_at.gt(from_time))
            .filter(feed::created_at.le(to_time))
            .filter(changes::entity_type.eq(entity_type))
            .filter(changes::entity_id.eq(entity_id).or(changes::entity_id.is_null()))
            .order(feed::created_at.asc())
            .load::<serde_json::Value>(*self.conn)
            .expect("Error querying feed events that change this entity")
            .into_iter()
            .map(|json| {
                serde_json::from_value(json)
                    .expect("Couldn't parse stored Feed event")
            })
            .collect()
    }
}