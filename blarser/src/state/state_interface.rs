use std::cmp::{max, min};
use std::iter;
use std::iter::Map;
use std::vec::IntoIter;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use diesel::{
    prelude::*,
    PgConnection,
    QueryDsl,
};
use itertools::Itertools;
use rocket::{info, info_};
use serde_json::Value;

use crate::sim::{self, Entity, FeedEventChangeResult};
use crate::api::EventuallyEvent;
use crate::schema::ingests::star;
use crate::state::{GenericEvent, GenericEventType};

pub struct StateInterface<'conn> {
    pub conn: &'conn &'conn mut PgConnection,
    pub ingest_id: i32,

    // TODO: Cache parameters
}

impl StateInterface<'_> {
    // Inclusive of start time, exclusive of end time
    pub fn versions<'a, EntityT: Entity + 'a>(&'a self, entity_id: Uuid, start_time: DateTime<Utc>, end_time: DateTime<Utc>)
                                              -> impl Iterator<Item=(DateTime<Utc>, EntityT)> + 'a {
        let start_entity: EntityT = self.entity(entity_id, start_time);
        let mut updates = self.version_updates(entity_id, start_time, end_time).peekable();
        self.events_for_entity(EntityT::name(), entity_id, start_time, end_time).into_iter()
            .scan(start_entity, move |entity, event| {
                // If there's an update before this event, replace the stored entity with it
                while let Some((next_update_time, _)) = updates.peek() {
                    if next_update_time < &event.time {
                        *entity = updates.next().unwrap().1;
                    }
                }

                // Apply the event, and yield the modified entity if the event was applicable.
                // The double-wrapping is because scan() uses the outer layer to stop iteration.
                match entity.apply_event(&event, self) {
                    FeedEventChangeResult::Ok => Some(Some((event.time, entity.clone()))),
                    FeedEventChangeResult::DidNotApply => Some(None)
                }
            })
            .flatten()
    }

    pub fn observed_versions<EntityT: Entity>(&self, entity_id: Uuid, start_time: DateTime<Utc>, end_time: DateTime<Utc>)
                                              -> Vec<(DateTime<Utc>, EntityT)> {
        let mut updates = self.version_updates(entity_id, start_time, end_time).collect_vec();
        if let Some((earliest_time, _)) = updates.first() {
            if earliest_time > &start_time {
                // Then there is no at the beginning, just return the vec
                return updates;
            }
        }

        // If the function hasn't returned yet, that means the version at start_time isn't being
        // fetched.
        let start_entity: EntityT = self.last_resolved_entity(entity_id, start_time).0;
        updates.insert(0, (start_time, start_entity));

        updates
    }

    fn version_updates<EntityT: Entity>(&self, entity_id: Uuid, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> impl Iterator<Item=(DateTime<Utc>, EntityT)> {
        use crate::schema::chron_updates::dsl;
        dsl::chron_updates
            .select((dsl::earliest_time, dsl::data))
            .filter(dsl::ingest_id.eq(self.ingest_id))
            .filter(dsl::entity_type.eq(EntityT::name()))
            .filter(dsl::entity_id.eq(entity_id))
            .filter(dsl::resolved.eq(true))
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
        info!("Computing {} {} at {}", EntityT::name(), entity_id, at_time);
        let (mut entity, event_start_time): (EntityT, _) = self.last_resolved_entity(entity_id, at_time);
        info!("Latest known entity: {}", event_start_time);
        let events = self.events_for_entity(EntityT::name(), entity_id, event_start_time, at_time);
        info!("{} potential events since last known entity", events.len());

        for event in events {
            info!("Applying {:?}", event.event_type);
            entity.apply_event(&event, self);
        }

        info!("Finished computing {} {}", EntityT::name(), entity_id);
        entity
    }

    // Inclusive of start time, exclusive of end time
    pub fn last_resolved_entity<EntityT: Entity>(&self, entity_id: Uuid, at_or_before: DateTime<Utc>) -> (EntityT, DateTime<Utc>) {
        use crate::schema::chron_updates::dsl;
        let (json, time) = dsl::chron_updates
            .select((dsl::data, dsl::latest_time))
            .filter(dsl::ingest_id.eq(self.ingest_id))
            .filter(dsl::entity_type.eq(EntityT::name()))
            .filter(dsl::entity_id.eq(entity_id))
            .filter(dsl::resolved.eq(true))
            // This has to be le (not lt)
            .filter(dsl::latest_time.le(at_or_before))
            .limit(1)
            .load::<(serde_json::Value, DateTime<Utc>)>(*self.conn)
            .expect("Error querying last known version of entity")
            .into_iter()
            .exactly_one()
            .expect("Expected exactly one response from query");

        let entity = serde_json::from_value(json)
            .expect("Error converting stored JSON into entity");

        (entity, time)
    }

    pub fn feed_events_for_entity(&self, entity_type: &str, entity_id: Uuid, from_time: DateTime<Utc>, to_time: DateTime<Utc>) -> Vec<EventuallyEvent> {
        use crate::schema::feed_events::dsl as feed;
        use crate::schema::feed_event_changes::dsl as changes;
        changes::feed_event_changes
            .inner_join(feed::feed_events)
            .select(feed::data)
            .filter(feed::ingest_id.eq(self.ingest_id))
            .filter(feed::created_at.ge(from_time))
            .filter(feed::created_at.lt(to_time))
            .filter(changes::entity_type.eq(entity_type))
            .filter(changes::entity_id.eq(entity_id).or(changes::entity_id.is_null()))
            .load::<serde_json::Value>(*self.conn)
            .expect("Error querying feed events that change this entity")
            .into_iter()
            .map(|json| {
                serde_json::from_value(json)
                    .expect("Couldn't parse stored Feed event")
            })
            .collect()
    }

    fn timed_events_for_entity(&self, entity_type: &str, _entity_id: Uuid, from_time: DateTime<Utc>, to_time: DateTime<Utc>) -> impl Iterator<Item=GenericEvent> {
        // observed_versions instead of versions to break an infinite recursion
        let mut versions = self.observed_versions::<sim::Sim>(Uuid::nil(), from_time, to_time).into_iter().peekable();
        info!("Fetched versions between {} and {}", from_time, to_time);
        let mut events = Vec::new();
        while let Some((entity_start_date, sim_)) = versions.next() {
            info!("{:?}", sim_);
            let entity_end_date = versions.peek().map(|(date, _)| date);

            // Simultaneously check if the time is in the range that the caller requested and the range
            // for which this sim_ is valid
            let start_date = max(from_time, entity_start_date);
            let end_date = match entity_end_date {
                Some(entity_end_date) => min(to_time, *entity_end_date),
                None => to_time,
            };

            info!("{}", start_date < sim_.earlseason_date);
            info!("{}, {} < {} ({})", sim_.earlseason_date < end_date, sim_.earlseason_date, end_date, to_time);
            info!("{}", (entity_type == "sim" || entity_type == "game"));
            info!("{}", entity_type == "sim");
            info!("{}", entity_type == "game");

            if start_date < sim_.earlseason_date && sim_.earlseason_date < end_date &&
                (entity_type == "sim" || entity_type == "game") {
                events.push(GenericEvent {
                    time: sim_.earlseason_date,
                    event_type: GenericEventType::EarlseasonStart,
                });
            }
        }

        events.into_iter().sorted_by_key(|e| e.time)
    }


    pub fn events_for_entity(&self, entity_type: &str, entity_id: Uuid, from_time: DateTime<Utc>, to_time: DateTime<Utc>) -> Vec<GenericEvent> {
        let feed_events = self.feed_events_for_entity(entity_type, entity_id, from_time, to_time);
        feed_events.into_iter()
            .map(|event| GenericEvent { time: event.created, event_type: GenericEventType::FeedEvent(event) })
            .chain(self.timed_events_for_entity(entity_type, entity_id, from_time, to_time))
            .sorted_by_key(|item| item.time)
            .collect()
    }
}