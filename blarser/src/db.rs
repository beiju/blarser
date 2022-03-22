use chrono::{DateTime, Utc};
use rocket_sync_db_pools::{database, diesel};
use serde::Serialize;

use rocket_sync_db_pools::diesel::prelude::*;
use crate::schema::*;

#[database("blarser")]
pub struct BlarserDbConn(diesel::PgConnection);

#[derive(Identifiable, Queryable, Serialize)]
pub struct Ingest {
    pub id: i32,
    pub started_at: DateTime<Utc>,
}

#[derive(Identifiable, Queryable, Debug, Serialize)]
pub struct Approval {
    pub id: i32,

    pub entity_type: String,
    pub entity_id: uuid::Uuid,
    pub perceived_at: DateTime<Utc>,

    pub message: String,
    pub approved: Option<bool>,
    pub explanation: Option<String>,
}

pub fn get_latest_ingest(conn: &diesel::PgConnection) -> Result<Option<Ingest>, diesel::result::Error> {
    use crate::schema::ingests::dsl::*;
    let latest_ingest: Vec<Ingest> = ingests
        .order(started_at.desc())
        .limit(1)
        .load(conn)?;
    Ok(latest_ingest.into_iter().next())
}

pub fn get_pending_approvals(conn: &diesel::PgConnection) -> Result<Vec<Approval>, diesel::result::Error> {
    use crate::schema::approvals::dsl as approvals;
    approvals::approvals
        .filter(approvals::approved.is_null())
        .load(conn)
}

pub fn set_approval(conn: &diesel::PgConnection, approval_id: i32, explanation: &str, approved: bool) -> Result<(), diesel::result::Error> {
    use crate::schema::approvals::dsl as approvals;
    diesel::update(approvals::approvals.find(approval_id))
        .set((
            approvals::approved.eq(approved),
            approvals::explanation.eq(explanation)
        ))
        .execute(conn)?;
    Ok(())
}