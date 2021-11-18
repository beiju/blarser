use rocket_sync_db_pools::{database, diesel};
use chrono::NaiveDateTime;
use crate::db_types::LogType;

use crate::schema::*;

#[database("blarser")]
pub struct BlarserDbConn(diesel::PgConnection);

#[derive(Insertable)]
#[table_name = "ingests"]
pub struct NewIngest {
    pub started_at: NaiveDateTime,
}

#[derive(Queryable)]
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