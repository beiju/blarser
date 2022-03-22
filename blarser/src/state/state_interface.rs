use chrono::{DateTime, Utc};
use diesel::{prelude::*, PgConnection, QueryResult, insert_into};
use log::info;
use uuid::Uuid;
use itertools::Itertools;
use serde_json::json;
use crate::api::EventuallyEvent;

use crate::entity;
use crate::entity::{TimedEvent, Entity, EntityTrait, EntityRawTrait};
use crate::events::{Event, EventTrait, EventAux};
use crate::ingest::{ChronObservationEvent, Observation};
use crate::state::{NewVersion, Version};
use crate::state::events_db::{DbEvent, EventEffect, EventSource, NewEvent, NewEventEffect, StoredEvent};
use crate::state::versions_db::{DbVersionWithEnd, NewVersionLink};

pub struct StateInterface<'conn> {
    conn: &'conn PgConnection,
    ingest_id: i32,
}

macro_rules! reader_with_id {
    ($fn_name:ident, $read_type:ty, $entity_type:expr) => {
        pub fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&self, id: Uuid, reader: Reader) -> QueryResult<Vec<ReadT>> {
            self.read_entity($entity_type, Some(id), reader, |entity| entity.try_into().unwrap())
        }
    };
}

macro_rules! reader_with_nil_id {
    ($fn_name:ident, $read_type:ty, $entity_type:expr) => {
        pub fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&self, reader: Reader) -> QueryResult<Vec<ReadT>> {
            self.read_entity($entity_type, Some(Uuid::nil()), reader, |entity| entity.try_into().unwrap())
        }
    };
}

impl<'conn> StateInterface<'conn> {
    pub fn new(conn: &'conn PgConnection, ingest_id: i32) -> Self {
        Self { conn, ingest_id }
    }

    fn read_entity<EntityT: EntityTrait, ReadT, Reader, Extractor>(
        &self,
        entity_type: &str,
        entity_id: Option<Uuid>,
        reader: Reader,
        extractor: Extractor,
    ) -> QueryResult<Vec<ReadT>>
        where ReadT: PartialEq,
              Reader: Fn(EntityT) -> ReadT,
              Extractor: Fn(Entity) -> EntityT {
        let mut unique_vals = Vec::new();
        let versions = self.get_versions_at_generic(entity_type, entity_id, None)?;

        assert!(!versions.is_empty(),
                "Error: There are no versions for the requested entity");

        for version in versions {
            let val = reader(extractor(version.entity));
            if !unique_vals.iter().any(|existing_val| existing_val == &val) {
                unique_vals.push(val);
            }
        }

        Ok(unique_vals)
    }

    reader_with_nil_id! {read_sim, entity::Sim, "sim"}
    reader_with_id! {read_player, entity::Player, "player"}
    reader_with_id! {read_team, entity::Team, "team"}
    reader_with_id! {read_game, entity::Game, "game"}
    reader_with_id! {read_standings, entity::Standings, "standings"}
    reader_with_id! {read_season, entity::Season, "season"}

    pub fn get_events_between(&self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> QueryResult<Vec<(StoredEvent, Vec<EventEffect>)>> {
        use crate::schema::events::dsl as events;

        let events = events::events
            .filter(events::ingest_id.eq(self.ingest_id))
            .filter(events::time.ge(start_time))
            .filter(events::time.lt(end_time))
            .get_results::<DbEvent>(self.conn)?;

        let effects = EventEffect::belonging_to(&events)
            .load::<EventEffect>(self.conn)?
            .grouped_by(&events);

        let events = events
            .into_iter()
            .map(|db_event| db_event.parse());

        Ok(events.zip(effects).collect())
    }

    pub fn get_versions_at_generic(&self, entity_type: &str, entity_id: Option<Uuid>, at_time: Option<DateTime<Utc>>) -> QueryResult<Vec<Version>> {
        use crate::schema::versions_with_end::dsl as versions;
        let base_query = versions::versions_with_end
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Has the right entity type (entity id handled below)
            .filter(versions::entity_type.eq(entity_type))
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
            .map(|db_version| db_version.parse())
            .collect();

        Ok(versions)
    }

    pub fn get_versions_at(&self, entity_type: &str, entity_id: Option<Uuid>, at_time: DateTime<Utc>) -> QueryResult<Vec<Version>> {
        self.get_versions_at_generic(entity_type, entity_id, Some(at_time))
    }

    pub fn get_latest_versions(&self, entity_type: &str, entity_id: Option<Uuid>) -> QueryResult<Vec<Version>> {
        self.get_versions_at_generic(entity_type, entity_id, None)
    }

    pub fn save_feed_event(&self, event: Event, effects: Vec<(String, Option<Uuid>, EventAux)>) -> QueryResult<StoredEvent> {
        use crate::schema::events::dsl as events;
        use crate::schema::event_effects::dsl as event_effects;

        let stored_event = insert_into(events::events)
            .values(NewEvent {
                ingest_id: self.ingest_id,
                time: event.time(),
                source: EventSource::Feed,
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
                    aux_data: serde_json::to_value(aux_data)
                        .expect("Error serializing EventAux data")
                }
            })
            .collect();

        insert_into(event_effects::event_effects)
            .values(insert_effects)
            .execute(self.conn)?;

        Ok(stored_event.parse())
    }

    pub fn add_initial_versions(&self, start_time: DateTime<Utc>, entities: impl Iterator<Item=Observation>) -> QueryResult<usize> {
        let from_event = self.add_start_event(start_time)?;

        let chunks = entities
            .map(move |observation| {
                let events = observation.entity_raw.init_events(start_time);
                let version = NewVersion {
                    ingest_id: self.ingest_id,
                    entity_type: observation.entity_raw.entity_type(),
                    entity_id: observation.entity_raw.entity_id(),
                    start_time,
                    entity: observation.entity_raw.to_json(),
                    from_event,
                    event_aux_data: json!(null),
                    observations: vec![start_time],
                };

                (version, events)
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

    pub fn save_successors(&self, successors: Vec<((Entity, EventAux), Vec<i32>)>, from_event: &StoredEvent) -> QueryResult<Vec<i32>> {
        let (new_versions, parents): (Vec<_>, Vec<_>) = successors.into_iter()
            .map(|((entity, aux), parents)| {
                let new_version = NewVersion {
                    ingest_id: self.ingest_id,
                    entity_type: entity.entity_type(),
                    entity_id: entity.entity_id(),
                    start_time: from_event.time,
                    entity: entity.to_json(),
                    from_event: from_event.id,
                    event_aux_data: serde_json::to_value(aux).expect("Error serializing event aux data"),
                    observations: vec![]
                };

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

    pub fn insert_events(&self, event: Vec<NewEvent>) -> QueryResult<usize> {
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
                        .expect("Failed to serialize Event")
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