use std::any::Any;
use std::cell::RefCell;
use chrono::{DateTime, Utc};
use diesel::{PgConnection};
use log::info;
use uuid::Uuid;

use crate::sim;
use crate::state::versions_db;
use crate::state::merged_successors::MergedSuccessors;

pub struct FeedStateInterface<'conn> {
    conn: &'conn PgConnection,
    ingest_id: i32,
    from_event: i32,
    at_time: DateTime<Utc>,

    // TODO: Cache parameters
}

pub struct EntityStateInterface<'conn, EntityT: sim::Entity> {
    conn: &'conn PgConnection,
    ingest_id: i32,
    at_time: DateTime<Utc>,
    entity_id: Uuid,
    versions: Vec<(bool, i32, EntityT)>,
    successors: RefCell<MergedSuccessors<(bool, EntityT)>>,
}

type ApplyResult<EntityT> = Result<Vec<EntityT>, anyhow::Error>;

macro_rules! reader_with_id {
    ($fn_name:ident, $read_type:path) => {
        fn $fn_name<ReadT: Eq, Reader: Fn($read_type) -> ReadT>(&self, id: Uuid, reader: Reader) -> Vec<ReadT> {
            self.read_entity(Some(id), reader)
        }
    };
}

macro_rules! reader_with_nil_id {
    ($fn_name:ident, $read_type:path) => {
        fn $fn_name<ReadT: Eq, Reader: Fn($read_type) -> ReadT>(&self, reader: Reader) -> Vec<ReadT> {
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
        where EntityT: sim::Entity, ReadT: Eq, Reader: Fn(EntityT) -> ReadT;
    fn with_entity<EntityT: 'static + sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&self, id: Option<Uuid>, f: F);

    reader_with_nil_id! {read_sim, sim::Sim}
    reader_with_id! {read_player, sim::Player}
    reader_with_id! {read_team, sim::Team}
    reader_with_id! {read_game, sim::Game}
    writer_with_nil_id! {with_sim, sim::Sim}
    writer_with_id! {with_player, sim::Player}
    writer_with_id! {with_team, sim::Team}
    writer_with_id! {with_game, sim::Game}
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

fn read_entity_common<EntityT: sim::Entity, ReadT, Reader>(c: &PgConnection, ingest_id: i32, at_time: DateTime<Utc>, id: Option<Uuid>, reader: Reader) -> Vec<ReadT> where ReadT: Eq, Reader: Fn(EntityT) -> ReadT {
    let mut unique_vals = Vec::new();
    let versions = versions_db::get_current_versions(c, ingest_id, EntityT::name(), id);

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
        where EntityT: sim::Entity, ReadT: Eq, Reader: Fn(EntityT) -> ReadT {
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

        versions_db::save_versions(&self.conn, self.ingest_id, self.from_event, self.at_time, all_successors.into_inner());
    }
}

impl<'conn, EntityT: sim::Entity> EntityStateInterface<'conn, EntityT> {
    pub fn new(c: &'conn PgConnection, ingest_id: i32, at_time: DateTime<Utc>, entity_id: Uuid, versions: Vec<(bool, i32, EntityT)>) -> EntityStateInterface<'conn, EntityT> {
        EntityStateInterface {
            conn: c,
            ingest_id,
            at_time,
            entity_id,
            versions,
            successors: RefCell::new(MergedSuccessors::new()),
        }
    }

    pub fn get_successors(self) -> Vec<((bool, EntityT), Vec<i32>)> {
        self.successors.into_inner().into_inner()
    }
}

impl<'conn, MainEntityT: 'static + sim::Entity> StateInterface for EntityStateInterface<'conn, MainEntityT> {
    fn read_entity<EntityT, ReadT, Reader>(&self, id: Option<Uuid>, reader: Reader) -> Vec<ReadT>
        where EntityT: sim::Entity, ReadT: Eq, Reader: Fn(EntityT) -> ReadT {
        read_entity_common(self.conn, self.ingest_id, self.at_time, id, reader)
    }

    fn with_entity<EntityT: 'static + sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&self, id: Option<Uuid>, f: F) {
        if id.map(|id| id != self.entity_id).unwrap_or(false) {
            return
        }

        for (a_bool, version_id, version) in &self.versions {
            // Round-trip through Any to get a type cast
            let version_any = version as &dyn Any;
            if let Some(version) = version_any.downcast_ref::<EntityT>() {
                match f(version.clone()) {
                    Ok(successors) => {
                        self.successors.borrow_mut().add_successors(*version_id, successors.into_iter().map(|successor| {
                            let successor_any = &successor as &dyn Any;
                            let successor = successor_any.downcast_ref::<MainEntityT>().unwrap();
                            (*a_bool, successor.clone())
                        }))
                    }
                    Err(_failure) => {
                        todo!()
                    }
                }
            }
        }

    }

}