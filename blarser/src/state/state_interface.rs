use chrono::{DateTime, Utc};
use diesel::{prelude::*, PgConnection, QueryResult, insert_into};
use itertools::Itertools;
use rocket::info;
use uuid::Uuid;
use serde_json::json;
use crate::api::EventuallyEvent;

use crate::{entity, with_any_entity, with_any_entity_raw};
use crate::entity::{TimedEvent, Entity, AnyEntity, EntityRaw};
use crate::events::{AnyEvent, Event, Start as StartEvent};
use crate::ingest::{ChronObservationEvent, Observation};
use crate::state::{NewVersion, Version};
use crate::state::events_db::{DbEvent, EventEffect, EventSource, NewEvent, NewEventEffect, StoredEvent};
use crate::state::versions_db::{DbVersionWithEnd, NewVersionLink};

pub struct StateInterface<'conn> {
    conn: &'conn PgConnection,
    ingest_id: i32,
}

macro_rules! reader_with_id {
    ($fn_name:ident, $read_type:ty) => {
        pub fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&self, id: Uuid, reader: Reader) -> QueryResult<Vec<ReadT>> {
            self.read_entity(Some(id), reader)
        }
    };
}

macro_rules! reader_with_nil_id {
    ($fn_name:ident, $read_type:ty) => {
        pub fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&self, reader: Reader) -> QueryResult<Vec<ReadT>> {
            self.read_entity(Some(Uuid::nil()), reader)
        }
    };
}


impl<'conn> StateInterface<'conn> {
    pub fn new(conn: &'conn PgConnection, ingest_id: i32) -> Self {
        Self { conn, ingest_id }
    }

    fn read_entity<EntityT: Entity, ReadT, Reader>(
        &self,
        entity_id: Option<Uuid>,
        reader: Reader,
    ) -> QueryResult<Vec<ReadT>>
        where ReadT: PartialEq,
              Reader: Fn(EntityT) -> ReadT {
        let mut unique_vals = Vec::new();
        let versions = self.get_versions_at_generic::<EntityT>(entity_id, None)?;

        assert!(!versions.is_empty(),
                "Error: There are no versions for the requested entity");

        for version in versions {
            let val = reader(version.entity);
            if !unique_vals.iter().any(|existing_val| existing_val == &val) {
                unique_vals.push(val);
            }
        }

        Ok(unique_vals)
    }

    reader_with_nil_id! {read_sim, entity::Sim }
    reader_with_id! {read_player, entity::Player }
    reader_with_id! {read_team, entity::Team }
    reader_with_id! {read_game, entity::Game }
    reader_with_id! {read_standings, entity::Standings }
    reader_with_id! {read_season, entity::Season }

    pub fn get_events_between(&self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> QueryResult<Vec<(StoredEvent, Vec<EventEffect>)>> {
        use crate::schema::events::dsl as events;

        let events = events::events
            .filter(events::ingest_id.eq(self.ingest_id))
            .filter(events::time.gt(start_time))
            .filter(events::time.le(end_time))
            .get_results::<DbEvent>(self.conn)?;

        let effects = EventEffect::belonging_to(&events)
            .load::<EventEffect>(self.conn)?
            .grouped_by(&events);

        let events = events
            .into_iter()
            .map(|db_event| db_event.parse());

        Ok(events.zip(effects).collect())
    }

    pub fn get_versions_at_generic<EntityT: Entity>(&self, entity_id: Option<Uuid>, at_time: Option<DateTime<Utc>>) -> QueryResult<Vec<Version<EntityT>>> {
        use crate::schema::versions_with_end::dsl as versions;
        let base_query = versions::versions_with_end
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Has the right entity type (entity id handled below)
            .filter(versions::entity_type.eq(EntityT::name()))
            // Has not been terminated
            .filter(versions::terminated.is_null())
            // Has the right end date/no end date
            .filter(versions::end_time.eq(at_time));

        let versions = match entity_id {
            Some(entity_id) => {
                base_query
                    // Has the right entity id
                    .filter(versions::entity_id.eq(entity_id))
                    .get_results::<DbVersionWithEnd>(self.conn)?
            }
            None => {
                base_query.get_results::<DbVersionWithEnd>(self.conn)?
            }
        };

        let versions = versions.into_iter()
            .map(|db_version| db_version.parse::<EntityT>())
            .collect();

        Ok(versions)
    }

    pub fn get_versions_at<EntityT: Entity>(&self, entity_id: Option<Uuid>, at_time: DateTime<Utc>) -> QueryResult<Vec<Version<EntityT>>> {
        self.get_versions_at_generic::<EntityT>(entity_id, Some(at_time))
    }

    pub fn get_latest_versions<EntityT: Entity>(&self, entity_id: Option<Uuid>) -> QueryResult<Vec<Version<EntityT>>> {
        self.get_versions_at_generic::<EntityT>(entity_id, None)
    }

    pub fn save_start_event(&self, event: AnyEvent, effects: Vec<(String, Option<Uuid>, serde_json::Value)>) -> QueryResult<StoredEvent> {
        self.save_event(EventSource::Start, event, effects)
    }

    pub fn save_feed_event(&self, event: AnyEvent, effects: Vec<(String, Option<Uuid>, serde_json::Value)>) -> QueryResult<StoredEvent> {
        self.save_event(EventSource::Feed, event, effects)
    }

    // This must take an AnyEvent, not generic EventT
    fn save_event(&self, source: EventSource, event: AnyEvent, effects: Vec<(String, Option<Uuid>, serde_json::Value)>) -> QueryResult<StoredEvent> {
        use crate::schema::events::dsl as events;
        use crate::schema::event_effects::dsl as event_effects;

        let stored_event = insert_into(events::events)
            .values(NewEvent {
                ingest_id: self.ingest_id,
                time: event.time(),
                source,
                data: serde_json::to_value(event)
                    .expect("Error serializing Event data"),
            })
            .returning(events::events::all_columns())
            .get_result::<DbEvent>(self.conn)?;

        let insert_effects: Vec<_> = effects.into_iter()
            .map(|(entity_type, entity_id, aux_data)| {
                NewEventEffect {
                    event_id: stored_event.id,
                    entity_type,
                    entity_id,
                    aux_data,
                }
            })
            .collect();

        insert_into(event_effects::event_effects)
            .values(insert_effects)
            .execute(self.conn)?;

        Ok(stored_event.parse())
    }

    fn version_from_start<EntityRawT: EntityRaw>(&self, entity_raw: EntityRawT, start_time: DateTime<Utc>, from_event: i32) -> (NewVersion, Vec<TimedEvent>) {
        let events = entity_raw.init_events(start_time);
        let version = NewVersion {
            ingest_id: self.ingest_id,
            entity_type: EntityRawT::name(),
            entity_id: entity_raw.id(),
            start_time,
            entity: serde_json::to_value(entity_raw)
                .expect("Error serializing EntityRaw"),
            from_event,
            event_aux_data: json!(null),
            observations: vec![start_time],
        };

        (version, events)
    }


    pub fn add_initial_versions(&self, start_time: DateTime<Utc>, entities: impl Iterator<Item=Observation>) -> QueryResult<usize> {
        let from_event = self.save_event(EventSource::Start, AnyEvent::Start(StartEvent::new(start_time)), Vec::new())?;

        let chunks = entities
            .map(move |observation| {
                with_any_entity_raw!(observation.entity_raw, raw => self.version_from_start(raw, start_time, from_event.id))
            })
            .chunks(2000); // Diesel can't handle inserting the whole thing in one go

        let mut inserted = 0;
        for chunk in &chunks {
            let (chunk_versions, chunk_events): (Vec<_>, Vec<_>) = chunk.unzip();

            self.insert_timed_events(chunk_events.into_iter().flatten())?;
            use crate::schema::versions::dsl as versions;
            inserted += insert_into(versions::versions)
                .values(chunk_versions)
                .execute(self.conn)?;
            info!("Inserted {} initial versions", inserted);
        }

        Ok::<_, diesel::result::Error>(inserted)
    }

    pub fn save_successors(&self, successors: Vec<((AnyEntity, serde_json::Value), Vec<i32>)>, start_time: DateTime<Utc>, from_event: i32) -> QueryResult<Vec<i32>> {
        let (new_versions, parents): (Vec<_>, Vec<_>) = successors.into_iter()
            .map(|((entity, event_aux_data), parents)| {
                let entity_type = entity.name();
                let new_version = with_any_entity!(entity, e => NewVersion {
                    ingest_id: self.ingest_id,
                    entity_type,
                    entity_id: e.id(),
                    start_time,
                    entity: serde_json::to_value(e).expect("Error serializing successor entity"),
                    from_event,
                    event_aux_data,
                    observations: vec![],
                });

                (new_version, parents)
            })
            .unzip();

        self.conn.transaction(|| {
            use crate::schema::versions::dsl as versions;
            use crate::schema::version_links::dsl as version_links;

            let inserted_versions = diesel::insert_into(versions::versions)
                .values(new_versions)
                .returning(versions::id)
                .get_results::<i32>(self.conn)?;

            let new_parents: Vec<_> = parents.into_iter().zip(&inserted_versions)
                .flat_map(|(parents, &child_id)| {
                    parents.into_iter().map(move |parent_id| {
                        NewVersionLink { parent_id, child_id }
                    })
                })
                .collect();

            diesel::insert_into(version_links::version_links)
                .values(new_parents)
                .execute(self.conn)?;

            Ok(inserted_versions)
        })
    }

    fn insert_event(&self, event: NewEvent) -> QueryResult<i32> {
        use crate::schema::events::dsl as events;

        insert_into(events::events)
            .values(event)
            .returning(events::id)
            .get_result::<i32>(self.conn)
    }

    fn insert_events(&self, event: Vec<NewEvent>) -> QueryResult<usize> {
        use crate::schema::events::dsl as events;

        insert_into(events::events)
            .values(event)
            .execute(self.conn)
    }

    pub fn add_start_event(&self, event_time: DateTime<Utc>) -> QueryResult<i32> {
        self.insert_event(NewEvent {
            ingest_id: self.ingest_id,
            time: event_time,
            source: EventSource::Start,
            data: serde_json::Value::Null,
        })
    }

    pub fn add_timed_event(&self, event: TimedEvent) -> QueryResult<i32> {
        self.insert_event(NewEvent {
            ingest_id: self.ingest_id,
            time: event.time,
            source: EventSource::Timed,
            data: serde_json::to_value(event.event_type)
                .expect("Error serializing TimedEvent"),
        })
    }

    pub fn insert_timed_events(&self, events: impl IntoIterator<Item=TimedEvent>) -> QueryResult<usize> {
        let events = events.into_iter()
            .map(|event| {
                NewEvent {
                    ingest_id: self.ingest_id,
                    time: event.time,
                    source: EventSource::Timed,
                    data: serde_json::to_value(event)
                        .expect("Failed to serialize Event"),
                }
            })
            .collect();

        self.insert_events(events)
    }

    pub fn add_feed_event(&self, event: EventuallyEvent) -> QueryResult<i32> {
        self.insert_event(NewEvent {
            ingest_id: self.ingest_id,
            time: event.created,
            source: EventSource::Feed,
            data: serde_json::to_value(event)
                .expect("Error serializing EventuallyEvent"),
        })
    }

    pub fn add_chron_event(&self, event: ChronObservationEvent) -> QueryResult<i32> {
        self.insert_event(NewEvent {
            ingest_id: self.ingest_id,
            time: event.applied_at,
            source: EventSource::Manual,
            data: serde_json::to_value(event)
                .expect("Error serializing ChronObservationEvent"),
        })
    }
}