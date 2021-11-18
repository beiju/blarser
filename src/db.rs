use rocket_sync_db_pools::{database, diesel};
use chrono::NaiveDateTime;

use crate::schema::ingests;

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