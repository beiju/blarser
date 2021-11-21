use rocket_sync_db_pools::{database, diesel};
use chrono::NaiveDateTime;
use serde::Serialize;
use crate::db_types::LogType;

use rocket_sync_db_pools::diesel::prelude::*;
use crate::schema::*;

#[database("blarser")]
pub struct BlarserDbConn(diesel::PgConnection);

#[derive(Insertable)]
#[table_name = "ingests"]
pub struct NewIngest {
    pub started_at: NaiveDateTime,
}

#[derive(Identifiable, Queryable, Serialize)]
pub struct Ingest {
    pub id: i32,
    pub started_at: NaiveDateTime,
}

#[derive(Insertable)]
#[table_name = "ingest_logs"]
pub struct NewIngestLog<'a> {
    pub at: NaiveDateTime,
    pub ingest_id: i32,
    pub type_: LogType,
    pub message: &'a str,
}

#[derive(Identifiable, Queryable, Associations, Debug, Serialize)]
#[belongs_to(Ingest)]
pub struct IngestLog {
    pub id: i32,
    pub at: NaiveDateTime,
    pub ingest_id: i32,
    pub type_: LogType,
    pub message: String
}

pub fn get_latest_ingest(conn: &diesel::PgConnection) -> Result<Option<Ingest>, diesel::result::Error> {
    use crate::schema::ingests::dsl::*;
    let latest_ingest: Vec<Ingest> = ingests
        .order(started_at.desc())
        .limit(1)
        .load(conn)?;
    Ok(latest_ingest.into_iter().nth(0))
}

pub fn get_logs_for(ingest: &Ingest, conn: &diesel::PgConnection) -> Result<Vec<IngestLog>, diesel::result::Error> {
    IngestLog::belonging_to(ingest)
        .load(conn)
}