use diesel::prelude::*;
use diesel::{Insertable, QueryDsl, RunQueryDsl, BelongingToDsl};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use itertools::{izip};

use crate::schema::*;
use crate::entity::Entity;
use crate::events::AnyEvent;
use crate::state::events_db::DbEvent;

#[derive(Insertable)]
#[table_name = "versions"]
pub struct NewVersion {
    pub ingest_id: i32,
    pub entity_type: &'static str,
    pub entity_id: Uuid,
    pub start_time: DateTime<Utc>,
    pub entity: serde_json::Value,
    pub from_event: i32,
    pub event_aux_data: serde_json::Value,
    pub observations: Vec<DateTime<Utc>>,
}

impl PartialEq for NewVersion {
    fn eq(&self, other: &Self) -> bool {
        self.ingest_id == other.ingest_id &&
            self.entity_type == other.entity_type &&
            self.entity_id == other.entity_id &&
            self.entity == other.entity &&
            self.from_event == other.from_event &&
            self.event_aux_data == other.event_aux_data &&
            self.observations == other.observations
    }
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[belongs_to(parent = "DbEvent", foreign_key = "from_event")]
#[table_name = "versions"]
pub(crate) struct DbVersion {
    pub id: i32,
    pub ingest_id: i32,

    pub entity_type: String,
    pub entity_id: Uuid,
    pub start_time: DateTime<Utc>,

    pub entity: serde_json::Value,
    pub from_event: i32,
    pub event_aux_data: serde_json::Value,

    pub observations: Vec<DateTime<Utc>>,
    pub terminated: Option<String>,
}

impl DbVersion {
    pub fn parse<EntityT: Entity>(self) -> Version<EntityT> {
        Version {
            id: self.id,
            ingest_id: self.ingest_id,
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            start_time: self.start_time,
            entity: serde_json::from_value(self.entity)
                .expect("Failed to parse entity from database"),
            from_event: self.from_event,
            event_aux_data: serde_json::from_value(self.event_aux_data)
                .expect("Failed to parse event aux info from database"),
            observations: self.observations,
            terminated: self.terminated,
        }
    }
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[belongs_to(parent = "DbEvent", foreign_key = "from_event")]
#[table_name = "versions"]
pub(crate) struct DbVersionWithEnd {
    pub id: i32,
    pub ingest_id: i32,

    pub entity_type: String,
    pub entity_id: Uuid,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,

    pub entity: serde_json::Value,
    pub from_event: i32,
    pub event_aux_data: serde_json::Value,

    pub observations: Vec<DateTime<Utc>>,
    pub terminated: Option<String>,
}

impl DbVersionWithEnd {
    pub fn parse<EntityT: Entity>(self) -> Version<EntityT> {
        let entity = serde_json::from_value(self.entity)
            .expect("Failed to parse version from database");
        Version {
            id: self.id,
            ingest_id: self.ingest_id,
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            start_time: self.start_time,
            entity,
            from_event: self.from_event,
            event_aux_data: serde_json::from_value(self.event_aux_data)
                .expect("Failed to parse event aux info from database"),
            observations: self.observations,
            terminated: self.terminated,
        }
    }
}

pub struct Version<EntityT> {
    pub id: i32,
    pub ingest_id: i32,

    pub entity_type: String,
    pub entity_id: Uuid,
    pub start_time: DateTime<Utc>,

    pub entity: EntityT,
    pub from_event: i32,
    pub event_aux_data: serde_json::Value,

    pub observations: Vec<DateTime<Utc>>,
    pub terminated: Option<String>,
}

#[derive(Insertable)]
#[table_name = "version_links"]
pub(crate) struct NewVersionLink {
    pub parent_id: i32,
    pub child_id: i32,
}

#[derive(Identifiable, Queryable, Associations, PartialEq, Debug)]
#[belongs_to(parent = "DbVersion", foreign_key = "child_id")]
#[belongs_to(parent = "DbVersionWithEnd", foreign_key = "child_id")]
#[table_name = "version_links"]
pub struct VersionLink {
    pub id: i32,
    pub parent_id: i32,
    pub child_id: i32,
}

pub fn get_entity_debug<EntityT: Entity>(c: &PgConnection, ingest_id: i32, entity_type: &str, entity_id: Uuid) -> QueryResult<Vec<(Version<EntityT>, AnyEvent, Vec<VersionLink>)>> {
    use crate::schema::versions::dsl as versions;
    use crate::schema::events::dsl as events;
    let (versions, events): (Vec<DbVersion>, Vec<DbEvent>) = versions::versions
        .inner_join(events::events.on(versions::from_event.eq(events::id)))
        // Is from the right ingest
        .filter(versions::ingest_id.eq(ingest_id))
        // Is the right entity
        .filter(versions::entity_type.eq(entity_type))
        .filter(versions::entity_id.eq(entity_id))
        .get_results::<(DbVersion, DbEvent)>(c)?
        .into_iter()
        .unzip();

    let parents = VersionLink::belonging_to(&versions)
        .load::<VersionLink>(c)?
        .grouped_by(&versions);

    let versions = versions.into_iter()
        .map(|version| version.parse::<EntityT>());

    let events = events.into_iter()
        .map(|event| event.parse().event);

    Ok(izip!(versions, events, parents).collect())
}

// pub fn get_events_for_entity_after(c: &PgConnection, ingest_id: i32, entity_type: &str, entity_id: Uuid, start_time: DateTime<Utc>) -> QueryResult<Vec<DbEvent>> {
//     use crate::schema::versions::dsl as versions;
//     use crate::schema::events::dsl as events;
//
//     versions::versions
//         .inner_join(events::events.on(versions::from_event.eq(events::id)))
//         // Is from the right ingest
//         .filter(versions::ingest_id.eq(ingest_id))
//         // Is the right entity
//         .filter(versions::entity_type.eq(entity_type))
//         .filter(versions::entity_id.eq(entity_id))
//         // Is after the desired time
//         .filter(events::event_time.gt(start_time))
//         // Just the event
//         .select(events::events::all_columns())
//         // No dupes
//         .distinct_on(events::id)
//         .get_results::<DbEvent>(c)
// }
//
// pub fn get_event_for_entity_preceding(c: &PgConnection, ingest_id: i32, entity_type: &str, entity_id: Uuid, start_time: DateTime<Utc>) -> QueryResult<DbEvent> {
//     use crate::schema::versions::dsl as versions;
//     use crate::schema::events::dsl as events;
//
//     versions::versions
//         .inner_join(events::events.on(versions::from_event.eq(events::id)))
//         // Is from the right ingest
//         .filter(versions::ingest_id.eq(ingest_id))
//         // Is the right entity
//         .filter(versions::entity_type.eq(entity_type))
//         .filter(versions::entity_id.eq(entity_id))
//         // Is before the desired time
//         .filter(events::event_time.le(start_time))
//         // Just the event
//         .select(events::events::all_columns())
//         // Just the most recent one
//         .order(events::event_time.desc())
//         .limit(1)
//         .get_result::<DbEvent>(c)
// }
//
// pub fn get_entity_update_tree(c: &PgConnection, ingest_id: i32, entity_type: &str, entity_id: Uuid, start_time: DateTime<Utc>) -> QueryResult<(Vec<DbEvent>, Vec<Vec<(Version, Vec<VersionLink>)>>)> {
//     use crate::schema::versions::dsl as versions;
//
//     let mut loaded_events = get_events_for_entity_after(c, ingest_id, entity_type, entity_id, start_time)?;
//
//     loaded_events.insert(0, get_event_for_entity_preceding(c, ingest_id, entity_type, entity_id, start_time)?);
//
//     let loaded_versions = Version::belonging_to(&loaded_events)
//         .filter(versions::entity_type.eq(entity_type))
//         .filter(versions::entity_id.eq(entity_id))
//         .filter(versions::terminated.is_null())
//         .load::<Version>(c)?;
//
//     let grouped_parents = VersionLink::belonging_to(&loaded_versions)
//         .load::<VersionLink>(c)?
//         .grouped_by(&loaded_versions);
//
//     let versions_with_parents = loaded_versions.into_iter()
//         .zip(grouped_parents)
//         .grouped_by(&loaded_events);
//
//     Ok((loaded_events, versions_with_parents))
// }


pub fn terminate_versions(c: &PgConnection, mut to_update: Vec<i32>, reason: String) -> QueryResult<()> {
    use crate::schema::versions::dsl as versions;

    #[derive(QueryableByName)]
    #[table_name = "versions"]
    struct VersionId {
        id: i32,
    }

    while !to_update.is_empty() {
        diesel::update(versions::versions.filter(versions::id.eq_any(to_update)))
            .set(versions::terminated.eq(Some(&reason)))
            .execute(c)?;

        to_update = diesel::sql_query("
            select v.id
            from versions v
                     join versions_parents vp on vp.child = v.id
                     join versions p on p.id = vp.parent
            where v.terminated is null
            group by v.id
            having count(*) = count(p.terminated)
        ").get_results::<VersionId>(c)?
            .into_iter()
            .map(|v| v.id)
            .collect();
    }

    Ok(())
}