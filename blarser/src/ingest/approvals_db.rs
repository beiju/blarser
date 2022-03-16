use chrono::{DateTime, Utc, Local};
use diesel::{ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, QueryResult, RunQueryDsl};
use uuid::Uuid;
use crate::db::IngestApproval;

use crate::schema::*;

#[derive(Insertable)]
#[table_name = "ingest_approvals"]
pub struct NewIngestApproval<'a> {
    pub at: DateTime<Utc>,
    pub chronicler_entity_type: &'a str,
    pub chronicler_time: DateTime<Utc>,
    pub chronicler_entity_id: uuid::Uuid,
    pub message: &'a str,
}

pub enum ApprovalState {
    Pending(i32),
    Approved(String),
    Rejected,
}

pub fn get_approval(c: &PgConnection, entity_type: &str, entity_id: Uuid, entity_time: DateTime<Utc>, message: &str) -> QueryResult<ApprovalState> {
    use crate::schema::ingest_approvals::dsl;

    let existing_approval = dsl::ingest_approvals
        .filter(dsl::chronicler_entity_type.eq(entity_type))
        .filter(dsl::chronicler_entity_id.eq(entity_id))
        .filter(dsl::chronicler_time.eq(entity_time))
        .filter(dsl::message.eq(message))
        .get_result::<IngestApproval>(c)
        .optional()?;

    if let Some(existing_approval) = existing_approval {
        if let Some(approved) = existing_approval.approved {
            let state = if approved {
                ApprovalState::Approved(
                    existing_approval.explanation
                        .expect("Approved approvals must have an explanation")
                )
            } else {
                ApprovalState::Rejected
            };
            Ok(state)
        } else {
            Ok(ApprovalState::Pending(existing_approval.id))
        }
    } else {
        let insert = NewIngestApproval {
            at: DateTime::from(Local::now()),
            chronicler_entity_type: entity_type,
            chronicler_entity_id: entity_id,
            chronicler_time: entity_time,
            message,
        };

        let approval_id = diesel::insert_into(dsl::ingest_approvals)
            .values(insert)
            .returning(dsl::id)
            .get_result::<i32>(c)?;

        Ok(ApprovalState::Pending(approval_id))
    }
}