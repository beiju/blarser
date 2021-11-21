use diesel::prelude::*;
use chrono::Utc;
use log::{info, debug};

use crate::db::{NewIngest, Ingest, BlarserDbConn, NewIngestLog};
use crate::db_types;


pub struct IngestLogger {
    ingest_id: i32,
    conn: BlarserDbConn,
}

impl IngestLogger {
    pub async fn new(conn: BlarserDbConn) -> diesel::QueryResult<IngestLogger> {
        use crate::schema::ingests::dsl::*;
        let this_ingest: Ingest = conn.run(move |c|
            diesel::insert_into(ingests).values(NewIngest {
                started_at: Utc::now().naive_utc()
            }).get_result(c)
        ).await?;

        info!("Starting ingest {} at {}", this_ingest.id, this_ingest.started_at);

        Ok(IngestLogger { ingest_id: this_ingest.id, conn })
    }

    pub async fn info(&self, msg: String) -> diesel::QueryResult<()> {
        info!("{}", msg);
        self.save_log(msg, db_types::LogType::Info).await
    }

    pub async fn debug(&self, msg: String) -> diesel::QueryResult<()> {
        debug!("{}", msg);
        self.save_log(msg, db_types::LogType::Debug).await
    }

    async fn save_log(&self, msg: String, type_: db_types::LogType) -> diesel::QueryResult<()> {
        use crate::schema::ingest_logs::dsl::ingest_logs;

        let ingest_id = self.ingest_id.clone();
        self.conn.run(move |c|
            diesel::insert_into(ingest_logs).values(NewIngestLog {
                at: Utc::now().naive_utc(),
                ingest_id,
                type_,
                message: &*msg
            }).execute(c)
        ).await?;

        Ok(())
    }
}