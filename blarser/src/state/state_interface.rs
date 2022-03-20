use std::cell::RefCell;
use chrono::{DateTime, Utc};
use diesel::{prelude::*, PgConnection, QueryResult};
use log::info;
use uuid::Uuid;

use crate::entity;
use crate::entity::{Entity, EntityTrait};
use crate::events::{Event, EventAux};
use crate::state::{NewVersion, Version, versions_db};
use crate::state::events_db::{DbEvent, EventEffect, StoredEvent};
use crate::state::merged_successors::MergedSuccessors;
use crate::state::versions_db::NewVersionLink;

pub struct StateInterface<'conn> {
    conn: &'conn PgConnection,
    ingest_id: i32,
}

impl<'conn> StateInterface<'conn> {
    pub fn new(conn: &'conn PgConnection, ingest_id: i32) -> Self {
        Self { conn, ingest_id }
    }

    pub fn get_events_between(&self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> QueryResult<Vec<(StoredEvent, Vec<EventEffect>)>> {
        use crate::schema::events::dsl as events;

        let events = events::events
            .filter(events::ingest_id.eq(self.ingest_id))
            .filter(events::time.ge(start_time))
            .filter(events::time.lt(end_time))
            .get_results::<DbEvent>(self.conn)?
            .into_iter()
            .map(|db_event| db_event.parse().expect("Error deserializing Event"))
            .collect();

        let effects = EventEffect::belonging_to(&events)
            .load(self.conn)?
            .group_by(&events);

        Ok(events.zip(effects))
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

        match entity_id {
            Some(entity_id) => {
                base_query
                    // Has the right entity id
                    .filter(versions::entity_id.eq(entity_id))
                    .get_results::<Version>(self.conn)
            }
            None => {
                base_query.get_results::<Version>(self.conn)
            }
        }
    }

    pub fn get_versions_at(&self, entity_type: &str, entity_id: Uuid, at_time: DateTime<Utc>) -> QueryResult<Vec<Version>> {
        self.get_versions_at_generic(entity_type, Some(entity_id), Some(at_time))
    }

    pub fn get_latest_versions(&self, entity_type: &str, entity_id: Uuid) -> QueryResult<Vec<Version>> {
        self.get_versions_at_generic(entity_type, Some(entity_id), None)
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
}