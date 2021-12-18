use diesel::prelude::*;
use chrono::{DateTime, Utc};
use log::{info, debug};
use uuid::Uuid;
use std::sync::mpsc;

use crate::db::{NewIngest, Ingest, NewIngestLog, NewIngestApproval, IngestApproval};
use crate::db_types;
use crate::ingest::IngestTask;


pub struct IngestLogger<'conn> {
    ingest_id: i32,
    conn: &'conn diesel::PgConnection,
    task: IngestTask,
}

impl<'conn> IngestLogger<'conn> {
    pub fn new(conn: &'conn diesel::PgConnection, task: IngestTask) -> diesel::QueryResult<IngestLogger<'conn>> {
        use crate::schema::ingests::dsl::*;

        let this_ingest: Ingest = diesel::insert_into(ingests).values(NewIngest {
            started_at: Utc::now().naive_utc()
        }).get_result(conn)?;

        let logger = IngestLogger { ingest_id: this_ingest.id, conn, task };
        logger.info(format!("Starting ingest {} at {}", this_ingest.id, this_ingest.started_at))?;

        Ok(logger)
    }

    pub fn info(&self, msg: String) -> diesel::QueryResult<()> {
        info!("{}", msg);
        self.save_log(msg, None, db_types::LogType::Info)
    }

    pub fn debug(&self, msg: String) -> diesel::QueryResult<()> {
        debug!("{}", msg);
        self.save_log(msg, None, db_types::LogType::Debug)
    }

    pub fn get_approval(
        &self,
        endpoint: &'static str,
        entity_id: Uuid,
        update_time: DateTime<Utc>,
        message: String,
    ) -> diesel::QueryResult<bool> {
        use crate::schema::ingest_approvals::dsl;

        let approval_record: IngestApproval = {
            let existing = dsl::ingest_approvals
                .filter(dsl::chronicler_entity_type.eq(endpoint))
                .filter(dsl::chronicler_time.eq(update_time.naive_utc()))
                .filter(dsl::chronicler_entity_id.eq(entity_id))
                .load::<IngestApproval>(self.conn)?;

            assert!(existing.len() <= 1, "Found more than one record for this approval");

            // into_iter().next() acts like first() but it moves the item out (and consumes the vec)
            if let Some(record) = existing.into_iter().next() {
                // If the message changed, update it
                if record.message != message {
                    diesel::update(&record)
                        .set(dsl::message.eq(&message))
                        .get_result(self.conn)
                } else {
                    Ok(record)
                }
            } else {
                diesel::insert_into(dsl::ingest_approvals).values(NewIngestApproval {
                    at: Utc::now().naive_utc(),
                    chronicler_entity_type: endpoint,
                    chronicler_time: update_time.naive_utc(),
                    chronicler_entity_id: entity_id.clone(),
                    message: &message,
                }).get_result(self.conn)
            }
        }?;

        loop {
            let (sender, receiver) = mpsc::channel();
            self.task.register_callback(approval_record.id, sender);

            // Check again for soundness
            match self.get_approval_from_db(endpoint, entity_id, update_time)? {
                Some(approval) => {
                    self.task.unregister_callback(approval_record.id);
                    return Ok(approval);
                }
                None => {}
            }

            let msg = format!("Waiting on approval for id {} from ingest {}", approval_record.id, self.ingest_id);
            info!("{}", msg);
            self.save_log(msg, Some(approval_record.id), db_types::LogType::Info)?;
            receiver.recv().unwrap();
        }
    }

    fn get_approval_from_db(&self, endpoint: &'static str, entity_id: Uuid, update_time: DateTime<Utc>) -> diesel::QueryResult<Option<bool>> {
        use crate::schema::ingest_approvals::dsl;
        let approvals = dsl::ingest_approvals
                .select(dsl::approved)
                .filter(dsl::chronicler_entity_type.eq(endpoint))
                .filter(dsl::chronicler_time.eq(update_time.naive_utc()))
                .filter(dsl::chronicler_entity_id.eq(entity_id))
                .load::<Option<bool>>(self.conn)?;

        assert!(approvals.len() <= 1, "Found more than one record for this approval");

        // Outer unwrap is whether Diesel found a record, inner unwrap is whether it was null
        // Can't flatten because
        Ok(approvals.into_iter().nth(0).flatten())
    }

    fn save_log(&self, msg: String, approval_id: Option<i32>, type_: db_types::LogType) -> diesel::QueryResult<()> {
        use crate::schema::ingest_logs::dsl::ingest_logs;

        let ingest_id = self.ingest_id.clone();

        diesel::insert_into(ingest_logs).values(NewIngestLog {
            at: Utc::now().naive_utc(),
            ingest_id,
            type_,
            message: &*msg,
            approval_id,
        }).execute(self.conn)?;

        Ok(())
    }

    pub fn increment_parsed_events(&self) -> diesel::QueryResult<()> {
        use crate::schema::ingests::dsl::*;

        let ingest_id = self.ingest_id.clone();
        diesel::update(ingests.filter(id.eq(ingest_id)))
            .set(events_parsed.eq(events_parsed + 1))
            .execute(self.conn)?;

        Ok(())
    }
}