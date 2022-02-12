use chrono::{DateTime, Utc};
use diesel::{PgConnection};
use itertools::all;
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

impl<'conn> StateInterface<'conn> {
    pub fn new(c: &'conn mut PgConnection, ingest_id: i32, at_time: DateTime<Utc>) -> StateInterface<'conn> {
        StateInterface {
            conn: c,
            ingest_id,
            at_time
        }
    }

    fn with_entity<EntityT: sim::Entity, F: Fn(EntityT) -> ApplyResult<EntityT>>(&mut self, entity_id: Uuid, f: F) {
        let versions = versions_db::get_possible_versions_at(&mut self.conn, self.ingest_id, EntityT::name(), entity_id, self.at_time);

        let mut all_successors = MergedSuccessors::new();
        for (version_id, version_json) in versions {
            let version: EntityT = serde_json::from_value(version_json)
                .expect("Couldn't deserialize stored entity version");

            match f(version) {
                Ok(successors) => {
                    all_successors.add_successors(version_id, successors)
                },
                Err(failure) => {
                    todo!()
                }
            }
        }

        if all_successors.is_empty() {
            // Show the user an approval, and if approved, add a "manual" event with these changes
            todo!()
        }

        versions_db::save_successors(&mut self.conn, self.ingest_id, self.at_time, all_successors.into_inner())
    }

    pub fn with_sim<F: Fn(sim::Sim) -> ApplyResult<sim::Sim>>(&mut self, f: F) {
        self.with_entity(Uuid::nil(), f)
    }
}