use chrono::{DateTime, Utc};
use diesel::{PgConnection};
use log::info;
use uuid::Uuid;
use crate::sim;
use crate::state::versions_db;
use crate::state::merged_successors::MergedSuccessors;

pub struct StateInterface<'conn> {
    conn: &'conn mut PgConnection,
    ingest_id: i32,
    at_time: DateTime<Utc>,

    // TODO: Cache parameters
}

type ApplyResult<EntityT> = Result<Vec<EntityT>, anyhow::Error>;

macro_rules! reader_with_id {
    ($fn_name:ident, $read_type:path) => {
        pub fn $fn_name<ReadT: Eq, Reader: Fn($read_type) -> ReadT>(&self, id: Uuid, reader: Reader) -> Vec<ReadT> {
            self.read_entity(Some(id), reader)
        }
    };
}

macro_rules! reader_with_nil_id {
    ($fn_name:ident, $read_type:path) => {
        pub fn $fn_name<ReadT: Eq, Reader: Fn($read_type) -> ReadT>(&self, reader: Reader) -> Vec<ReadT> {
            self.read_entity(Some(Uuid::nil()), reader)
        }
    };
}

macro_rules! writer_each {
    ($fn_name:ident, $entity_type:path) => {
        pub fn $fn_name<F: Fn($entity_type) -> ApplyResult<$entity_type>>(&self, f: F) {
            self.with_entity(None, f)
        }
    };
}

macro_rules! writer_with_id {
    ($fn_name:ident, $entity_type:path) => {
        pub fn $fn_name<F: Fn($entity_type) -> ApplyResult<$entity_type>>(&self, id: Uuid, f: F) {
            self.with_entity(Some(id), f)
        }
    };
}

macro_rules! writer_with_nil_id {
    ($fn_name:ident, $entity_type:path) => {
        pub fn $fn_name<F: Fn($entity_type) -> ApplyResult<$entity_type>>(&self, f: F) {
            self.with_entity(Some(Uuid::nil()), f)
        }
    };
}

impl<'conn> StateInterface<'conn> {
    pub fn new(c: &'conn mut PgConnection, ingest_id: i32, at_time: DateTime<Utc>) -> StateInterface<'conn> {
        StateInterface {
            conn: c,
            ingest_id,
            at_time,
        }
    }

    pub fn read_entity<EntityT, ReadT, Reader>(&self, id: Option<Uuid>, reader: Reader) -> Vec<ReadT>
        where EntityT: sim::Entity, ReadT: Eq, Reader: Fn(EntityT) -> ReadT {
        let mut unique_vals = Vec::new();
        let versions = versions_db::get_possible_versions_at(&self.conn, self.ingest_id, EntityT::name(), id, self.at_time);

        assert!(!versions.is_empty(),
                "Error: There are no versions for the requested entity");

        for (_, version_json) in versions {
            let version: EntityT = serde_json::from_value(version_json)
                .expect("Couldn't deserialize stored entity version");

            let val = reader(version);
            if !unique_vals.iter().any(|existing_val| existing_val == &val) {
                unique_vals.push(val);
            }
        }

        unique_vals
    }

    reader_with_nil_id! {read_sim, sim::Sim}
    reader_with_id! {read_player, sim::Player}
    reader_with_id! {read_team, sim::Team}

    fn with_entity<EntityT: sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&self, id: Option<Uuid>, f: F) {
        let versions = versions_db::get_possible_versions_at(&self.conn, self.ingest_id, EntityT::name(), id, self.at_time);

        let mut all_successors = MergedSuccessors::new();
        for (version_id, version_json) in versions {
            let version: EntityT = serde_json::from_value(version_json)
                .expect("Couldn't deserialize stored entity version");

            match f(version) {
                Ok(successors) => {
                    all_successors.add_successors(version_id, successors)
                }
                Err(failure) => {
                    todo!()
                }
            }
        }

        if all_successors.is_empty() {
            // Show the user an approval, and if approved, add a "manual" event with these changes
            todo!()
        }

        versions_db::save_successors(&self.conn, self.ingest_id, self.at_time, all_successors.into_inner())
    }

    // TODO Probably should change these to be named writer or something
    writer_with_nil_id! {with_sim, sim::Sim}
    writer_with_id! {with_player, sim::Player}
    writer_with_id! {with_team, sim::Team}
    writer_with_id! {with_game, sim::Game}
    writer_each! {with_each_game, sim::Game}
}