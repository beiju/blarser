use std::collections::HashMap;
use chrono::{DateTime, Utc};
use diesel::{prelude::*, dsl, PgConnection, QueryResult, insert_into};
use itertools::Itertools;
use rocket::info;
use serde::Serialize;
use uuid::Uuid;
use serde_json::json;
use partial_information::PartialInformationCompare;
use std::iter;

use diesel::sql_types;


use crate::events::{AnyEvent, Event, Start};
use crate::entity::{self, Entity, AnyEntity, EntityRaw};
// use crate::events::{AnyEvent, Start as StartEvent};
use crate::ingest::Observation;
use crate::state::{EntityType, ApprovalState, NewVersion, Version, VersionLink};
use crate::state::approvals_db::NewApproval;
use crate::state::events_db::{DbEvent, EventEffect, EventSource, NewEvent, NewEventEffect};
use crate::state::versions_db::{DbVersionWithEnd, NewVersionLink};

use crate::schema::versions_with_end::dsl as versions_dsl;

pub struct StateInterface<'conn> {
    conn: &'conn mut PgConnection,
    ingest_id: i32,
}

#[derive(Serialize)]
pub struct EntityDescription {
    entity_type: EntityType,
    entity_id: Uuid,
    description: String,
}

#[derive(Serialize, Queryable)]
#[serde(rename_all = "camelCase")]
pub struct VersionDebug {
    pub id: i32,
    pub start_time: DateTime<Utc>,
    pub event: serde_json::Value,
    pub event_aux: serde_json::Value,
    pub entity: serde_json::Value,
    pub terminated: Option<String>,
    pub observations: Vec<DateTime<Utc>>,
}

#[derive(Serialize, Queryable)]
#[serde(rename_all = "camelCase")]
pub struct VersionLinkDebug {
    pub parent_id: i32,
    pub child_id: i32,
}

#[derive(Serialize)]
pub struct EntityVersionsDebug {
    pub edges: Vec<VersionLinkDebug>,
    pub nodes: HashMap<i32, VersionDebug>,
}

macro_rules! reader_with_id {
    ($fn_name:ident, $read_type:ty) => {
        pub fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&mut self, id: Uuid, reader: Reader) -> QueryResult<Vec<ReadT>> {
            self.read_entity(Some(id), reader)
        }
    };
}

macro_rules! flat_reader_with_id {
    ($fn_name:ident, $read_type:ty) => {
        pub fn $fn_name<ReadT: PartialEq, IterT: IntoIterator<Item=ReadT>, Reader: Fn($read_type) -> IterT>(
            &mut self, id: Uuid, reader: Reader
        ) -> QueryResult<Vec<ReadT>> {
            self.read_entity_flat(Some(id), reader)
        }
    };
}

macro_rules! reader_with_nil_id {
    ($fn_name:ident, $read_type:ty) => {
        pub fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&mut self, reader: Reader) -> QueryResult<Vec<ReadT>> {
            self.read_entity(Some(Uuid::nil()), reader)
        }
    };
}
sql_function! {
    #[aggregate]
    fn array_agg(expr: sql_types::Integer) -> sql_types::Array<sql_types::Integer>;
}

sql_function! {
    fn coalesce(x: sql_types::Nullable<sql_types::Array<sql_types::Integer>>, y: sql_types::Array<sql_types::Integer>) -> sql_types::Array<sql_types::Integer>;
}

pub type Effects = Vec<(EntityType, Option<Uuid>, serde_json::Value)>;

fn raw_to_full_json<EntityT: Entity + PartialInformationCompare>(raw_json: serde_json::Value) -> serde_json::Result<serde_json::Value> {
    let raw: EntityT::Raw = serde_json::from_value(raw_json)?;
    let full = EntityT::from_raw(raw);
    serde_json::to_value(full)
}

impl<'conn> StateInterface<'conn> {
    pub fn new(conn: &'conn mut PgConnection, ingest_id: i32) -> Self {
        Self { conn, ingest_id }
    }

    // fn read_entity<EntityT: Entity, ReadT, Reader>(
    //     &mut self,
    //     entity_id: Option<Uuid>,
    //     reader: Reader,
    // ) -> QueryResult<Vec<ReadT>>
    //     where ReadT: PartialEq,
    //           Reader: Fn(EntityT) -> ReadT {
    //     let mut unique_vals = Vec::new();
    //     let versions = self.get_versions_at_generic::<EntityT>(entity_id, None)?;
    //
    //     assert!(!versions.is_empty(),
    //             "Error: There are no versions for the requested entity");
    //
    //     for version in versions {
    //         let val = reader(version.entity);
    //         if !unique_vals.iter().any(|existing_val| existing_val == &val) {
    //             unique_vals.push(val);
    //         }
    //     }
    //
    //     Ok(unique_vals)
    // }
    //
    // fn read_entity_flat<EntityT: Entity, ReadT, IterT, Reader>(
    //     &mut self,
    //     entity_id: Option<Uuid>,
    //     reader: Reader,
    // ) -> QueryResult<Vec<ReadT>>
    //     where ReadT: PartialEq,
    //           Reader: Fn(EntityT) -> IterT,
    //           IterT: IntoIterator<Item=ReadT> {
    //     let mut unique_vals = Vec::new();
    //     let versions = self.get_versions_at_generic::<EntityT>(entity_id, None)?;
    //
    //     assert!(!versions.is_empty(),
    //             "Error: There are no versions for the requested entity");
    //
    //     for version in versions {
    //         let vals = reader(version.entity);
    //         for val in vals {
    //             if !unique_vals.iter().any(|existing_val| existing_val == &val) {
    //                 unique_vals.push(val);
    //             }
    //         }
    //     }
    //
    //     Ok(unique_vals)
    // }
    //
    // reader_with_nil_id! { read_sim, entity::Sim }
    // reader_with_id! { read_player, entity::Player }
    // reader_with_id! { read_team, entity::Team }
    // reader_with_id! { read_game, entity::Game }
    // reader_with_id! { read_standings, entity::Standings }
    // reader_with_id! { read_season, entity::Season }
    // flat_reader_with_id! { read_game_flat, entity::Game }
    //
    // pub fn get_events_between(&self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> QueryResult<Vec<(StoredEvent, Vec<EventEffect>)>> {
    //     use crate::schema::events::dsl as events;
    //
    //     let events = events::events
    //         .filter(events::ingest_id.eq(self.ingest_id))
    //         .filter(events::time.gt(start_time))
    //         .filter(events::time.le(end_time))
    //         .get_results::<DbEvent>(self.conn)?;
    //
    //     let effects = EventEffect::belonging_to(&events)
    //         .load::<EventEffect>(self.conn)?
    //         .grouped_by(&events);
    //
    //     let events = events
    //         .into_iter()
    //         .map(|db_event| db_event.parse());
    //
    //     Ok(events.zip(effects).collect())
    // }
    //
    // pub fn get_events_for_versions_after<EntityRawT: EntityRaw>(&self, entity_raw: &EntityRawT, after_time: DateTime<Utc>) -> QueryResult<Vec<(StoredEvent, Vec<EventEffect>)>> {
    //     use crate::schema::events::dsl as events;
    //     use crate::schema::event_effects::dsl as event_effects;
    //
    //     let all_events_for_entity = events::events
    //         .inner_join(event_effects::event_effects.on(event_effects::event_id.eq(events::id)))
    //         // Is from the right ingest
    //         .filter(events::ingest_id.eq(self.ingest_id))
    //         // Is for the right entity type
    //         .filter(event_effects::entity_type.eq(EntityRawT::name()))
    //         // Is for the right entity id (null entity id = all entities of that type)
    //         .filter(event_effects::entity_id.eq(entity_raw.id()).or(event_effects::entity_id.is_null()));
    //
    //     // I assume these could be unioned somehow, and the documentation says it works, but the
    //     // compiler disagrees and I'm not inclined to investigate that right now.
    //     let active_event = all_events_for_entity
    //         // Is before or at the requested time
    //         .filter(events::time.le(after_time))
    //         // Select just the latest one
    //         .order(events::time.desc())
    //         .limit(1)
    //         .get_result::<(DbEvent, EventEffect)>(self.conn)?;
    //
    //     let later_events = all_events_for_entity
    //         // Is strictly after the requested time
    //         .filter(events::time.gt(after_time))
    //         // Select all in ascending order
    //         .order(events::time.asc())
    //         .get_results::<(DbEvent, EventEffect)>(self.conn)?;
    //
    //     let (mut events, effects): (Vec<_>, Vec<_>) = iter::once(active_event)
    //         .chain(later_events)
    //         .unzip();
    //
    //     events.dedup();
    //
    //     let effects = effects.grouped_by(&events);
    //
    //     let events = events.into_iter()
    //         .map(DbEvent::parse);
    //
    //     Ok(events.zip(effects).collect())
    // }
    //
    pub fn get_versions_at_generic<EntityT: Entity>(&mut self, entity_type: EntityType, entity_id: Option<Uuid>, at_time: Option<DateTime<Utc>>) -> QueryResult<Vec<Version<EntityT>>> {
        use crate::schema::versions_with_end::dsl as versions;
        let base_query = versions::versions_with_end
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Has the right entity type (entity id handled below)
            .filter(versions::entity_type.eq(entity_type))
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

    pub fn get_versions_at<EntityT: Entity>(&mut self, entity_type: EntityType, entity_id: Option<Uuid>, at_time: DateTime<Utc>) -> QueryResult<Vec<Version<EntityT>>> {
        self.get_versions_at_generic::<EntityT>(entity_type, entity_id, Some(at_time))
    }

    // pub fn get_latest_versions<EntityT: Entity>(&mut self, entity_id: Option<Uuid>) -> QueryResult<Vec<Version<EntityT>>> {
    //     self.get_versions_at_generic::<EntityT>(entity_id, None)
    // }
    //
    // pub fn get_versions_for_entity_raw_between<EntityRawT: EntityRaw>(&self, entity_raw: &EntityRawT, time_start: DateTime<Utc>, time_end: DateTime<Utc>) -> QueryResult<Vec<(i32, Vec<(Version<EntityRawT::Entity>, Vec<VersionLink>)>)>> {
    //     use crate::schema::versions_with_end::dsl as versions;
    //     let versions_query = self.query_versions_with_end(EntityRawT::name(), entity_raw.id())
    //         // Has not been terminated
    //         .filter(versions::terminated.is_null())
    //         // Version's range ends after the requested range starts
    //         .filter(versions::end_time.gt(time_start).or(versions::end_time.is_null()))
    //         // Version's range starts at or before the requested range ends
    //         .filter(versions::start_time.le(time_end))
    //         // Order by time
    //         .order(versions::from_event);
    //
    //     // info!("Query: {}", diesel::debug_query(&versions_query));
    //     let versions = versions_query
    //         .get_results::<DbVersionWithEnd>(self.conn)?;
    //
    //     let version_links = VersionLink::belonging_to(&versions)
    //         .get_results::<VersionLink>(self.conn)?
    //         .grouped_by(&versions);
    //
    //     // I couldn't figure out how to do this with simple iterators
    //     let mut versions_grouped = Vec::new();
    //     let mut iter = versions.into_iter().zip(version_links).peekable();
    //     while let Some((first_version, links)) = iter.next() {
    //         let group_event = first_version.from_event;
    //         let mut group = vec![(first_version.parse(), links)];
    //
    //         while let Some((version, _)) = iter.peek() {
    //             if version.from_event == group_event {
    //                 let (version, links) = iter.next().unwrap();
    //                 group.push((version.parse(), links));
    //             } else {
    //                 break;
    //             }
    //         }
    //
    //         versions_grouped.push((group_event, group));
    //     }
    //
    //     Ok(versions_grouped)
    // }
    //
    // pub fn save_start_event(&self, event: AnyEvent, effects: Effects) -> QueryResult<(StoredEvent, Vec<EventEffect>)> {
    //     self.save_event(EventSource::Start, event, effects)
    // }
    //
    // pub fn save_feed_event(&self, event: AnyEvent, effects: Effects) -> QueryResult<(StoredEvent, Vec<EventEffect>)> {
    //     self.save_event(EventSource::Feed, event, effects)
    // }
    //
    // pub fn save_timed_event(&self, event: AnyEvent, effects: Effects) -> QueryResult<(StoredEvent, Vec<EventEffect>)> {
    //     self.save_event(EventSource::Timed, event, effects)
    // }
    //
    // This must take an AnyEvent, not generic EventT
    fn save_event(&mut self, source: EventSource, event: AnyEvent, effects: Effects) -> QueryResult<(i32, Vec<EventEffect>)> {
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

        let effects = insert_into(event_effects::event_effects)
            .values(insert_effects)
            .returning(event_effects::event_effects::all_columns())
            .get_results::<EventEffect>(self.conn)?;

        Ok((stored_event.id, effects))
    }

    fn version_from_start(ingest_id: i32, observation: Observation, start_time: DateTime<Utc>, from_event: i32) -> NewVersion {
        // let events = entity_raw.init_events(start_time);
        let version = NewVersion {
            ingest_id,
            entity_type: observation.entity_type,
            entity_id: observation.entity_id,
            start_time,
            entity: match observation.entity_type {
                EntityType::Sim => { raw_to_full_json::<entity::Sim>(observation.entity_json) }
                EntityType::Player => { raw_to_full_json::<entity::Player>(observation.entity_json) }
                EntityType::Team => { raw_to_full_json::<entity::Team>(observation.entity_json) }
                EntityType::Game => { raw_to_full_json::<entity::Game>(observation.entity_json) }
                EntityType::Standings => { raw_to_full_json::<entity::Standings>(observation.entity_json) }
                EntityType::Season => { raw_to_full_json::<entity::Season>(observation.entity_json) }
            }.expect("Error round-tripping JSON from raw to full"),
            from_event,
            event_aux_data: json!(null),
            observations: vec![start_time],
        };

        version
    }

    pub fn add_initial_versions(&mut self, start_time: DateTime<Utc>, entities: impl Iterator<Item=Observation>) -> QueryResult<usize> {
        let (from_event, _) = self.save_event(EventSource::Start, Start::new(start_time).into(), Vec::new())?;

        let ingest_id = self.ingest_id;
        let chunks = entities
            .map(move |observation| {
                StateInterface::version_from_start(ingest_id, observation, start_time, from_event)
            })
            .chunks(2000); // Diesel can't handle inserting the whole thing in one go

        let mut inserted = 0;
        for chunk in &chunks {
            use crate::schema::versions::dsl as versions;
            inserted += insert_into(versions::versions)
                .values(chunk.collect::<Vec<_>>())
                .execute(self.conn)?;
            info!("Inserted {} initial versions", inserted);
        }

        Ok::<_, diesel::result::Error>(inserted)
    }

    // pub fn save_successors(&self, successors: impl IntoIterator<Item=((AnyEntity, serde_json::Value, Vec<DateTime<Utc>>), Vec<i32>)>, start_time: DateTime<Utc>, from_event: i32) -> QueryResult<Vec<i32>> {
    //     let (new_versions, parents): (Vec<_>, Vec<_>) = successors.into_iter()
    //         .map(|((entity, event_aux_data, observations), parents)| {
    //             let entity_type = entity.name();
    //             let new_version = with_any_entity!(entity, e => NewVersion {
    //                 ingest_id: self.ingest_id,
    //                 entity_type,
    //                 entity_id: e.id(),
    //                 start_time,
    //                 entity: serde_json::to_value(e).expect("Error serializing successor entity"),
    //                 from_event,
    //                 event_aux_data,
    //                 observations,
    //             });
    //
    //             (new_version, parents)
    //         })
    //         .unzip();
    //
    //     self.conn.transaction(|_| {
    //         use crate::schema::versions::dsl as versions;
    //         use crate::schema::version_links::dsl as version_links;
    //
    //         let inserted_versions = diesel::insert_into(versions::versions)
    //             .values(new_versions)
    //             .returning(versions::id)
    //             .get_results::<i32>(self.conn)?;
    //
    //         let new_parents: Vec<_> = parents.into_iter().zip(&inserted_versions)
    //             .flat_map(|(parents, &child_id)| {
    //                 parents.into_iter().map(move |parent_id| {
    //                     NewVersionLink { parent_id, child_id }
    //                 })
    //             })
    //             .collect();
    //
    //         diesel::insert_into(version_links::version_links)
    //             .values(new_parents)
    //             .execute(self.conn)?;
    //
    //         Ok(inserted_versions)
    //     })
    // }

    pub fn upsert_approval(&mut self, entity_type: EntityType, entity_id: Uuid, perceived_at: DateTime<Utc>, message: &str) -> QueryResult<ApprovalState> {
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

    pub fn terminate_versions(&mut self, mut to_update: Vec<i32>, reason: String) -> QueryResult<()> {
        use crate::schema::versions::dsl as versions;

        #[derive(QueryableByName)]
        #[table_name = "versions"]
        struct VersionId {
            id: i32,
        }

        while !to_update.is_empty() {
            diesel::update(versions::versions.filter(versions::id.eq_any(to_update)))
                .set(versions::terminated.eq(Some(&reason)))
                .execute(self.conn)?;

            to_update = diesel::sql_query("
            select v.id
            from versions v
                     join version_links vp on vp.child_id = v.id
                     join versions p on p.id = vp.parent_id
            where v.terminated is null
            group by v.id
            having count(*) = count(p.terminated)
        ").get_results::<VersionId>(self.conn)?
                .into_iter()
                .map(|v| v.id)
                .collect();
        }

        Ok(())
    }

    pub fn get_recently_updated_entity_descriptions(&mut self, limit: i64) -> QueryResult<Vec<EntityDescription>> {
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
            .get_results::<(EntityType, Uuid, serde_json::Value)>(self.conn)?
            .into_iter()
            .map(|(entity_type, entity_id, entity_json)| {
                // let description = entity_description(&entity_type, entity_json);
                let description = "TODO".to_string();

                EntityDescription {
                    entity_type,
                    entity_id,
                    description,
                }
            })
            .collect();

        Ok(result)
    }

    fn query_versions_with_end(&self, entity_type: EntityType, entity_id: Uuid) ->
    dsl::FindBy<dsl::FindBy<dsl::FindBy<versions_dsl::versions_with_end, versions_dsl::ingest_id, i32>, versions_dsl::entity_type, EntityType>, versions_dsl::entity_id, Uuid> {
        use crate::schema::versions_with_end::dsl as versions;

        versions::versions_with_end
            // Is from the right ingest
            .filter(versions::ingest_id.eq(self.ingest_id))
            // Is the requested entity type
            .filter(versions::entity_type.eq(entity_type))
            // Is the requested entity id
            .filter(versions::entity_id.eq(entity_id))
    }

    pub fn get_entity_debug(&mut self, entity_type: EntityType, entity_id: Uuid) -> QueryResult<EntityVersionsDebug> {
        use crate::schema::versions_with_end::dsl as versions;
        use crate::schema::events::dsl as events;
        use crate::schema::version_links::dsl as version_links;

        let nodes = self.query_versions_with_end(entity_type, entity_id)
            // Database constraints should ensure inner join and left join are identical
            .inner_join(events::events.on(events::id.eq(versions::from_event)))
            .select((
                versions::id,
                versions::start_time,
                events::data,
                versions::event_aux_data,
                versions::entity,
                versions::terminated,
                versions::observations
            ))
            .get_results::<VersionDebug>(self.conn)?
            .into_iter()
            .map(|version| (version.id, version))
            .collect();

        let edges = self.query_versions_with_end(entity_type, entity_id)
            .inner_join(version_links::version_links.on(version_links::parent_id.eq(versions::id)))
            .select((version_links::parent_id, version_links::child_id))
            .get_results::<VersionLinkDebug>(self.conn)?;

        Ok(EntityVersionsDebug { edges, nodes })
    }
}