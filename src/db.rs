use std::collections::HashMap;
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
    pub events_parsed: i32,
}
#[derive(Insertable)]
#[table_name = "ingest_approvals"]
pub struct NewIngestApproval<'a> {
    pub at: NaiveDateTime,
    pub chronicler_entity_type: &'a str,
    pub chronicler_time: NaiveDateTime,
    pub chronicler_entity_id: uuid::Uuid,
    pub message: &'a str,
}

#[derive(Identifiable, Queryable, Debug, Serialize)]
pub struct IngestApproval {
    pub id: i32,
    pub at: NaiveDateTime,
    pub chronicler_entity_type: String,
    pub chronicler_time: NaiveDateTime,
    pub chronicler_entity_id: uuid::Uuid,
    pub message: String,
    pub approved: Option<bool>,
    pub explanation: Option<String>,
}

#[derive(Insertable)]
#[table_name = "ingest_logs"]
pub struct NewIngestLog<'a> {
    pub at: NaiveDateTime,
    pub ingest_id: i32,
    pub type_: LogType,
    pub message: &'a str,
    pub approval_id: Option<i32>,
}

#[derive(Identifiable, Queryable, Associations, Debug, Serialize)]
#[belongs_to(Ingest, IngestApproval)]
pub struct IngestLog {
    pub id: i32,
    pub at: NaiveDateTime,
    pub ingest_id: i32,
    pub type_: LogType,
    pub message: String,
    pub approval_id: Option<i32>,
}

#[derive(Serialize)]
pub struct IngestLogAndApproval {
    pub log: IngestLog,
    pub approval: Option<IngestApproval>
}

pub fn get_latest_ingest(conn: &diesel::PgConnection) -> Result<Option<Ingest>, diesel::result::Error> {
    use crate::schema::ingests::dsl::*;
    let latest_ingest: Vec<Ingest> = ingests
        .order(started_at.desc())
        .limit(1)
        .load(conn)?;
    Ok(latest_ingest.into_iter().nth(0))
}

pub fn get_logs_for(ingest: &Ingest, conn: &diesel::PgConnection) -> Result<Vec<IngestLogAndApproval>, diesel::result::Error> {
    use crate::schema::ingest_approvals::dsl::*;
    let logs: Vec<IngestLog> = IngestLog::belonging_to(ingest)
        .load(conn)?;

    let approval_ids = logs.iter().filter_map(|log| log.approval_id).collect::<Vec<_>>();

    let approvals: Vec<IngestApproval> = ingest_approvals
        .filter(id.eq(diesel::dsl::any(approval_ids)))
        .filter(approved.is_null())
        .load(conn)?;

    let mut approvals: HashMap<i32, IngestApproval> = approvals.into_iter()
        .map(|approval| (approval.id, approval))
        .collect();

    let merged: Vec<IngestLogAndApproval> = logs.into_iter()
        .map(|log| {
            let approval = log.approval_id.as_ref().and_then(|approval_id| approvals.remove(approval_id));
            IngestLogAndApproval { log, approval }
        })
        .collect();

    Ok(merged)
}

pub fn get_pending_approvals(conn: &diesel::PgConnection) -> Result<Vec<IngestApproval>, diesel::result::Error> {
    use crate::schema::ingest_approvals::dsl::*;
    ingest_approvals
        .filter(approved.is_null())
        .load(conn)
}

pub fn set_approval(conn: &diesel::PgConnection, approval_id: i32, message: &str, approved: bool) -> Result<(), diesel::result::Error> {
    use crate::schema::ingest_approvals::dsl;
    diesel::update(dsl::ingest_approvals.find(approval_id))
        .set((
            dsl::approved.eq(approved),
            dsl::message.eq(message)
        ))
        .execute(conn)?;
    Ok(())
}