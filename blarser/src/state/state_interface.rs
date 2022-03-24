use std::collections::HashMap;
use chrono::{DateTime, Utc};
use diesel::{prelude::*, PgConnection, QueryResult, insert_into};
use itertools::Itertools;
use rocket::info;
use serde::Serialize;
use uuid::Uuid;
use serde_json::json;
use partial_information::PartialInformationCompare;

use crate::{entity, with_any_entity, with_any_entity_raw};
use crate::entity::{Entity, AnyEntity, EntityRaw, entity_description};
use crate::events::{AnyEvent, Start as StartEvent};
use crate::ingest::Observation;
use crate::state::{ApprovalState, NewVersion, Version, VersionLink};
use crate::state::approvals_db::NewApproval;
use crate::state::events_db::{DbEvent, EventEffect, EventSource, NewEvent, NewEventEffect, StoredEvent};
use crate::state::versions_db::{DbVersionWithEnd, NewVersionLink};

pub struct StateInterface<'conn> {
    conn: &'conn PgConnection,
    ingest_id: i32,
}

#[derive(Serialize)]
pub struct EntityDescription {
    entity_type: String,
    entity_id: Uuid,
    description: String,
}

#[derive(Serialize, Queryable)]
#[serde(rename_all = "camelCase")]
pub struct VersionDebug {
    pub id: i32,
    pub parent_ids: Vec<i32>,
    pub start_time: DateTime<Utc>,
    pub event: serde_json::Value,
    pub event_aux: serde_json::Value,
    pub entity: serde_json::Value,
    pub entity_diff: Option<serde_json::Value>,
    pub terminated: Option<String>,
    pub observations: Vec<DateTime<Utc>>,
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

use diesel::sql_types;
sql_function! {
    #[aggregate]
    fn array_agg(expr: sql_types::Integer) -> sql_types::Array<sql_types::Integer>;
}

sql_function! {
    fn coalesce(x: sql_types::Nullable<sql_types::Array<sql_types::Integer>>, y: sql_types::Array<sql_types::Integer>) -> sql_types::Array<sql_types::Integer>;
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

    pub fn get_events_for_entity_raw_after<EntityRawT: EntityRaw>(&self, entity_raw: &EntityRawT, after_time: DateTime<Utc>) -> QueryResult<Vec<(StoredEvent, Vec<EventEffect>)>> {
        use crate::schema::events::dsl as events;
        use crate::schema::event_effects::dsl as event_effects;

        let (mut events, effects): (Vec<_>, Vec<_>) = events::events
            .inner_join(event_effects::event_effects.on(event_effects::event_id.eq(events::id)))
            // Is from the right ingest
            .filter(events::ingest_id.eq(self.ingest_id))
            // Is for the right entity type
            .filter(event_effects::entity_type.eq(EntityRawT::name()))
            // Is for the right entity id (null entity id = all entities of that type)
            .filter(event_effects::entity_id.eq(entity_raw.id()).or(event_effects::entity_id.is_null()))
            // Is after the requested time. Needs to be ge, not gt
            .filter(events::time.ge(after_time))
            .order(events::time.asc())
            .get_results::<(DbEvent, EventEffect)>(self.conn)?
            .into_iter()
            .unzip();

        events.dedup();

        let effects = effects.grouped_by(&events);

        let events = events.into_iter()
            .map(DbEvent::parse);

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
            // This ends up being (is null or is null) in the None case but the second is_null is
            // needed for the Some case
            .filter(versions::end_time.eq(at_time).or(versions::end_time.is_null()));

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

    pub fn get_versions_for_entity_raw_between<EntityRawT: EntityRaw>(&self, entity_raw: &EntityRawT, time_start: DateTime<Utc>, time_end: DateTime<Utc>) -> QueryResult<Vec<(i32, Vec<(Version<EntityRawT::Entity>, Vec<VersionLink>)>)>> {
        use crate::schema::versions_with_end::dsl as versions;
        let versions = versions::versions_with_end
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Has the right entity type
            .filter(versions::entity_type.eq(EntityRawT::name()))
            // Has the right entity id
            .filter(versions::entity_id.eq(entity_raw.id()))
            // Has not been terminated
            .filter(versions::terminated.is_null())
            // Version's range ends after the requested range starts
            .filter(versions::end_time.gt(time_start).or(versions::end_time.is_null()))
            // Version's range starts at or before the requested range ends
            .filter(versions::start_time.le(time_end))
            // Order by time
            .order(versions::from_event)
            .get_results::<DbVersionWithEnd>(self.conn)?;

        let version_links = VersionLink::belonging_to(&versions)
            .get_results::<VersionLink>(self.conn)?
            .grouped_by(&versions);

        // I couldn't figure out how to do this with simple iterators
        let mut versions_grouped = Vec::new();
        let mut iter = versions.into_iter().zip(version_links).peekable();
        while let Some((first_version, links)) = iter.next() {
            let group_event = first_version.from_event;
            let mut group = vec![(first_version.parse(), links)];

            while let Some((version, _)) = iter.peek() {
                if version.from_event == group_event {
                    let (version, links) = iter.next().unwrap();
                    group.push((version.parse(), links));
                } else {
                    break;
                }
            }

            versions_grouped.push((group_event, group));
        }

        Ok(versions_grouped)
    }

    pub fn save_start_event(&self, event: AnyEvent, effects: Vec<(String, Option<Uuid>, serde_json::Value)>) -> QueryResult<StoredEvent> {
        self.save_event(EventSource::Start, event, effects)
    }

    pub fn save_feed_event(&self, event: AnyEvent, effects: Vec<(String, Option<Uuid>, serde_json::Value)>) -> QueryResult<StoredEvent> {
        self.save_event(EventSource::Feed, event, effects)
    }

    pub fn save_timed_event(&self, event: AnyEvent, effects: Vec<(String, Option<Uuid>, serde_json::Value)>) -> QueryResult<StoredEvent> {
        self.save_event(EventSource::Timed, event, effects)
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

    fn version_from_start<EntityRawT: EntityRaw>(&self, entity_raw: EntityRawT, start_time: DateTime<Utc>, from_event: i32) -> (NewVersion, Vec<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)>) {
        let events = entity_raw.init_events(start_time);
        let version = NewVersion {
            ingest_id: self.ingest_id,
            entity_type: EntityRawT::name(),
            entity_id: entity_raw.id(),
            start_time,
            entity: serde_json::to_value(<EntityRawT::Entity as PartialInformationCompare>::from_raw(entity_raw))
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

            // It's not worth the optimization to save multiple at once (it will literally never be
            // used unless I need to add more init events)
            for (event, effects) in chunk_events.into_iter().flatten() {
                self.save_feed_event(event, effects)?;
            }
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

    pub fn upsert_approval(&self, entity_type: &str, entity_id: Uuid, perceived_at: DateTime<Utc>, message: &str) -> QueryResult<ApprovalState> {
        use crate::schema::approvals::dsl as approvals;

        let (id, approved) = diesel::insert_into(approvals::approvals)
            .values(NewApproval { entity_type, entity_id, perceived_at, message })
            .on_conflict((approvals::entity_type, approvals::entity_id, approvals::perceived_at))
            .do_update()
            .set(approvals::message.eq(message))
            .returning((approvals::id, approvals::approved))
            .get_result::<(i32, Option<bool>)>(self.conn)?;

        if let Some(approved) = approved {
            if approved {
                Ok(ApprovalState::Approved)
            } else {
                Ok(ApprovalState::Rejected)
            }
        } else {
            Ok(ApprovalState::Pending(id))
        }
    }

    pub fn get_recently_updated_entity_descriptions(&self, limit: i64) -> QueryResult<Vec<EntityDescription>> {
        use crate::schema::versions_with_end::dsl as versions;
        let result = versions::versions_with_end
            .select((versions::entity_type, versions::entity_id, versions::entity))
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Is a latest version
            .filter(versions::end_time.is_null())
            // Get the most recently updated ones
            .order(versions::start_time.desc())
            .limit(limit)
            .get_results::<(String, Uuid, serde_json::Value)>(self.conn)?
            .into_iter()
            .map(|(entity_type, entity_id, entity_json)| {
                let description = entity_description(&entity_type, entity_json);

                EntityDescription {
                    entity_type,
                    entity_id,
                    description,
                }
            })
            .collect();

        Ok(result)
    }

    pub fn get_entity_debug(&self, entity_type: &str, entity_id: Uuid) -> QueryResult<Vec<VersionDebug>> {
        use crate::schema::versions_with_end::dsl as versions;
        use crate::schema::events::dsl as events;
        use crate::schema::version_links::dsl as version_links;

        let results = versions::versions_with_end
            // Database constraints should ensure inner join and left join are identical
            .inner_join(events::events.on(events::id.eq(versions::from_event)))
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Is the requested entity type
            .filter(versions::entity_type.eq(entity_type))
            // Is the requested entity id
            .filter(versions::entity_id.eq(entity_id))
            .select((
                versions::id,
                coalesce(
                    version_links::version_links
                        .filter(version_links::child_id.eq(versions::id))
                        .select(array_agg(version_links::parent_id))
                        .single_value(),
                    diesel::dsl::sql("'{}'"),
                ),
                versions::start_time,
                events::data,
                versions::event_aux_data,
                versions::entity,
                diesel::dsl::sql::<sql_types::Nullable<sql_types::Jsonb>>("null"), // Gets filled in with entity_diff later
                versions::terminated,
                versions::observations
            ))
            .get_results::<VersionDebug>(self.conn)?;
        //
        // let id_map: HashMap<_, _> = results.iter()
        //     .enumerate()
        //     .map(|(i, version)| (version.id, i))
        //     .collect();
        //
        // for mut version in results {
        //
        // }

        Ok(results)
    }
}