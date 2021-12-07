use diesel::prelude::*;
use chrono::{DateTime, Utc};
use log::{info, debug};
use uuid::Uuid;
use tokio::sync::oneshot;

use crate::db::{NewIngest, Ingest, BlarserDbConn, NewIngestLog, NewIngestApproval, IngestApproval};
use crate::db_types;
use crate::ingest::IngestTask;


pub struct IngestLogger {
    ingest_id: i32,
    conn: BlarserDbConn,
    task: IngestTask,
}

impl IngestLogger {
    pub async fn new(conn: BlarserDbConn, task: IngestTask) -> diesel::QueryResult<IngestLogger> {
        use crate::schema::ingests::dsl::*;
        let this_ingest: Ingest = conn.run(move |c|
            diesel::insert_into(ingests).values(NewIngest {
                started_at: Utc::now().naive_utc()
            }).get_result(c)
        ).await?;

        info!("Starting ingest {} at {}", this_ingest.id, this_ingest.started_at);

        Ok(IngestLogger { ingest_id: this_ingest.id, conn, task })
    }

    pub async fn info(&self, msg: String) -> diesel::QueryResult<()> {
        info!("{}", msg);
        self.save_log(msg, None,db_types::LogType::Info).await
    }

    pub async fn debug(&self, msg: String) -> diesel::QueryResult<()> {
        debug!("{}", msg);
        self.save_log(msg, None,db_types::LogType::Debug).await
    }

    pub async fn get_approval(
        &self,
        endpoint: &'static str,
        entity_id: Uuid,
        update_time: DateTime<Utc>,
        message: String
    ) -> diesel::QueryResult<bool> {
        use crate::schema::ingest_approvals::dsl;

        let approval_record: IngestApproval = self.conn.run(move |c| {
            let existing = dsl::ingest_approvals
                .filter(dsl::chronicler_entity_type.eq(endpoint))
                .filter(dsl::chronicler_time.eq(update_time.naive_utc()))
                .filter(dsl::chronicler_entity_id.eq(entity_id))
                .load::<IngestApproval>(c)?;

            assert!(existing.len() <= 1, "Found more than one record for this approval");

            // into_iter().next() acts like first() but it moves the item out (and consumes the vec)
            if let Some(record) = existing.into_iter().next() {
                return Ok(record)
            }

            diesel::insert_into(dsl::ingest_approvals).values(NewIngestApproval {
                at: Utc::now().naive_utc(),
                chronicler_entity_type: endpoint,
                chronicler_time: update_time.naive_utc(),
                chronicler_entity_id: entity_id.clone(),
                message: &message,
            }).get_result(c)
        }).await?;

        loop {
            let (sender, receiver) = oneshot::channel();
            self.task.register_callback(approval_record.id, sender);

            // Check again for soundness
            match self.get_approval_from_db(endpoint, entity_id, update_time).await? {
                Some(approval) => {
                    self.task.unregister_callback(approval_record.id);
                    return Ok(approval)
                },
                None => {}
            }

            let msg = format!("Waiting on approval for id {} from ingest {}", approval_record.id, self.ingest_id);
            info!("{}", msg);
            self.save_log(msg, Some(approval_record.id),db_types::LogType::Info).await?;
            receiver.await.unwrap();
        }
    }

    async fn get_approval_from_db(&self, endpoint: &'static str, entity_id: Uuid, update_time: DateTime<Utc>) -> diesel::QueryResult<Option<bool>> {
        use crate::schema::ingest_approvals::dsl;
        let approvals = self.conn.run(move |c|
            dsl::ingest_approvals
                .select(dsl::approved)
                .filter(dsl::chronicler_entity_type.eq(endpoint))
                .filter(dsl::chronicler_time.eq(update_time.naive_utc()))
                .filter(dsl::chronicler_entity_id.eq(entity_id))
                .load::<Option<bool>>(c)
        ).await?;

        assert!(approvals.len() <= 1, "Found more than one record for this approval");

        // Outer unwrap is whether Diesel found a record, inner unwrap is whether it was null
        // Can't flatten because
        Ok(approvals.into_iter().nth(0).flatten())
    }

    async fn save_log(&self, msg: String, approval_id: Option<i32>, type_: db_types::LogType) -> diesel::QueryResult<()> {
        use crate::schema::ingest_logs::dsl::ingest_logs;

        let ingest_id = self.ingest_id.clone();
        self.conn.run(move |c|
            diesel::insert_into(ingest_logs).values(NewIngestLog {
                at: Utc::now().naive_utc(),
                ingest_id,
                type_,
                message: &*msg,
                approval_id,
            }).execute(c)
        ).await?;

        Ok(())
    }
}