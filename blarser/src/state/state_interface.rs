use std::cell::RefCell;
use chrono::{DateTime, Utc};
use diesel::{PgConnection};
use log::info;
use uuid::Uuid;

use crate::sim;
use crate::state::{Version, versions_db};
use crate::state::merged_successors::MergedSuccessors;

pub struct FeedStateInterface<'conn> {
    conn: &'conn PgConnection,
    ingest_id: i32,
    from_event: i32,
    at_time: DateTime<Utc>,

    // TODO: Cache parameters
}

pub struct EntityStateInterface<'conn> {
    conn: &'conn PgConnection,
    at_time: DateTime<Utc>,
    version: Version,
    successors: RefCell<Vec<(serde_json::Value, Option<DateTime<Utc>>)>>,
}

type ApplyResult<EntityT> = Result<Vec<EntityT>, anyhow::Error>;

macro_rules! reader_with_id {
    ($fn_name:ident, $read_type:path) => {
        fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&self, id: Uuid, reader: Reader) -> Vec<ReadT> {
            self.read_entity(Some(id), reader)
        }
    };
}

macro_rules! reader_with_nil_id {
    ($fn_name:ident, $read_type:path) => {
        fn $fn_name<ReadT: PartialEq, Reader: Fn($read_type) -> ReadT>(&self, reader: Reader) -> Vec<ReadT> {
            self.read_entity(Some(Uuid::nil()), reader)
        }
    };
}

macro_rules! writer_each {
    ($fn_name:ident, $entity_type:path) => {
        fn $fn_name<F: Fn($entity_type) -> ApplyResult<$entity_type>>(&self, f: F) {
            self.with_entity(None, f)
        }
    };
}

macro_rules! writer_with_id {
    ($fn_name:ident, $entity_type:path) => {
        fn $fn_name<F: Fn($entity_type) -> ApplyResult<$entity_type>>(&self, id: Uuid, f: F) {
            self.with_entity(Some(id), f)
        }
    };
}

macro_rules! writer_with_nil_id {
    ($fn_name:ident, $entity_type:path) => {
        fn $fn_name<F: Fn($entity_type) -> ApplyResult<$entity_type>>(&self, f: F) {
            self.with_entity(Some(Uuid::nil()), f)
        }
    };
}

pub trait StateInterface {
    fn read_entity<EntityT, ReadT, Reader>(&self, id: Option<Uuid>, reader: Reader) -> Vec<ReadT>
        where EntityT: sim::Entity, ReadT: PartialEq, Reader: Fn(EntityT) -> ReadT;
    fn with_entity<EntityT: 'static + sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&self, id: Option<Uuid>, f: F);

    reader_with_nil_id! {read_sim, sim::Sim}
    reader_with_id! {read_player, sim::Player}
    reader_with_id! {read_team, sim::Team}
    reader_with_id! {read_game, sim::Game}
    reader_with_id! {read_standings, sim::Standings}
    reader_with_id! {read_season, sim::Season}
    writer_with_nil_id! {with_sim, sim::Sim}
    writer_with_id! {with_player, sim::Player}
    writer_with_id! {with_team, sim::Team}
    writer_with_id! {with_game, sim::Game}
    writer_with_id! {with_standings, sim::Standings}
    writer_with_id! {with_season, sim::Season}
    writer_each! {with_each_game, sim::Game}
}

impl<'conn> FeedStateInterface<'conn> {
    pub fn new(c: &'conn PgConnection, ingest_id: i32, from_event: i32, at_time: DateTime<Utc>) -> FeedStateInterface<'conn> {
        FeedStateInterface {
            conn: c,
            ingest_id,
            from_event,
            at_time,
        }
    }
}

fn read_entity_common<EntityT: sim::Entity, ReadT, Reader>(c: &PgConnection, ingest_id: i32, at_time: DateTime<Utc>, id: Option<Uuid>, reader: Reader) -> Vec<ReadT> where ReadT: PartialEq, Reader: Fn(EntityT) -> ReadT {
    let mut unique_vals = Vec::new();
    let versions = versions_db::get_possible_versions_at(c, ingest_id, EntityT::name(), id, at_time);

    assert!(!versions.is_empty(),
            "Error: There are no versions for the requested entity");

    for (_, version_json, version_time) in versions {
        assert!(version_time <= at_time);
        let version: EntityT = serde_json::from_value(version_json)
            .expect("Couldn't deserialize stored entity version");

        let val = reader(version);
        if !unique_vals.iter().any(|existing_val| existing_val == &val) {
            unique_vals.push(val);
        }
    }

    unique_vals
}


impl<'conn> StateInterface for FeedStateInterface<'conn> {
    fn read_entity<EntityT, ReadT, Reader>(&self, id: Option<Uuid>, reader: Reader) -> Vec<ReadT>
        where EntityT: sim::Entity, ReadT: PartialEq, Reader: Fn(EntityT) -> ReadT {
        read_entity_common::<EntityT, _, _>(self.conn, self.ingest_id, self.at_time, id, reader)
    }

    fn with_entity<EntityT: sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&self, id: Option<Uuid>, f: F) {
        if let Some(id) = id {
            info!("Updating {} {}", EntityT::name(), id);
        } else {
            info!("Updating all {} entities", EntityT::name());
        }
        let versions = versions_db::get_current_versions(&self.conn, self.ingest_id, EntityT::name(), id);
        assert!(!versions.is_empty(), "Tried to modify an entity/entity type that doesn't have any entries");

        let mut all_successors = MergedSuccessors::new();
        for (version_id, version_json, version_time) in versions {
            assert!(version_time <= self.at_time);
            let version: EntityT = serde_json::from_value(version_json)
                .expect("Couldn't deserialize stored entity version");

            match f(version) {
                Ok(successors) => {
                    all_successors.add_successors(version_id, successors)
                }
                Err(_failure) => {
                    todo!()
                }
            }
        }

        if all_successors.is_empty() {
            // Show the user an approval, and if approved, add a "manual" event with these changes
            todo!()
        }

        versions_db::save_versions_from_entities(&self.conn, self.ingest_id, self.from_event, self.at_time, all_successors.into_inner())
            .expect("Error saving successors to database");
    }
}

impl<'conn> EntityStateInterface<'conn> {
    pub fn new(c: &'conn PgConnection, at_time: DateTime<Utc>, version: Version) -> EntityStateInterface<'conn> {
        EntityStateInterface {
            conn: c,
            at_time,
            version,
            successors: RefCell::new(Vec::new()),
        }
    }

    pub fn get_successors(self) -> Vec<(serde_json::Value, Option<DateTime<Utc>>)> {
        self.successors.into_inner()
    }
}

impl<'conn> StateInterface for EntityStateInterface<'conn> {
    fn read_entity<EntityT, ReadT, Reader>(&self, id: Option<Uuid>, reader: Reader) -> Vec<ReadT>
        where EntityT: sim::Entity, ReadT: PartialEq, Reader: Fn(EntityT) -> ReadT {
        read_entity_common(self.conn, self.version.ingest_id, self.at_time, id, reader)
    }

    fn with_entity<EntityT: 'static + sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&self, id: Option<Uuid>, f: F) {
        if EntityT::name() != self.version.entity_type {
            return
        }

        if id.map(|id| id != self.version.entity_id).unwrap_or(false) {
            return
        }

        let entity = serde_json::from_value(self.version.data.clone())
            .expect("Couldn't deserialize stored entity data");

        match f(entity) {
            Ok(successors) => {
                self.successors.borrow_mut().extend(successors.into_iter().map(|successor| {
                    let next_timed_event = successor.next_timed_event(self.at_time).map(|e| e.time);
                    let json = serde_json::to_value(successor).expect("Error serializing successor version");
                    (json, next_timed_event)
                }));
            }
            Err(_failure) => {
                todo!()
            }
        }

    }

}