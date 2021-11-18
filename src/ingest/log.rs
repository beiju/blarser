use diesel::prelude::*;
use chrono::Utc;
use log::info;

use crate::db::{NewIngest, Ingest, BlarserDbConn, NewIngestLog};
use crate::db_types::LogType;


pub struct IngestLog(i32);

impl IngestLog {
    pub async fn new(conn: &BlarserDbConn) -> diesel::QueryResult<IngestLog> {
        use crate::schema::ingests::dsl::*;
        let this_ingest: Ingest = conn.run(move |c|
            diesel::insert_into(ingests).values(NewIngest {
                started_at: Utc::now().naive_utc()
            }).get_result(c)
        ).await?;

        info!("Starting ingest {} at {}", this_ingest.id, this_ingest.started_at);

        Ok(IngestLog(this_ingest.id))
    }

    pub async fn info(self, conn: &BlarserDbConn, msg: String) -> diesel::QueryResult<()> {
        use crate::schema::ingest_logs::dsl::*;

        info!("{}", msg);

        conn.run(move |c|
            diesel::insert_into(ingest_logs).values(NewIngestLog {
                at: Utc::now().naive_utc(),
                ingest_id: self.0,
                type_: LogType::Info,
                message: &*msg
            }).execute(c)
        ).await?;

        Ok(())

    }
}