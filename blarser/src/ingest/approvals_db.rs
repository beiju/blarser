use chrono::{DateTime, Utc, Local};
use diesel::{ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, QueryResult, RunQueryDsl};
use uuid::Uuid;
use crate::db::Approval;

use crate::schema::*;

#[derive(Insertable)]
#[table_name = "approvals"]
pub struct NewApproval<'a> {
    pub entity_type: &'a str,
    pub entity_id: uuid::Uuid,
    pub perceived_at: DateTime<Utc>,
    pub message: &'a str,
}

pub enum ApprovalState {
    Pending(i32),
    Approved(String),
    Rejected,
}

pub fn get_approval(c: &PgConnection, entity_type: &str, entity_id: Uuid, entity_time: DateTime<Utc>, message: &str) -> QueryResult<ApprovalState> {
    use crate::schema::approvals::dsl as approvals;

    let existing_approval = approvals::approvals
        .filter(approvals::entity_type.eq(entity_type))
        .filter(approvals::entity_id.eq(entity_id))
        .filter(approvals::perceived_at.eq(entity_time))
        .filter(approvals::message.eq(message))
        .get_result::<Approval>(c)
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
        let insert = NewApproval {
            entity_type,
            entity_id,
            perceived_at: entity_time,
            message,
        };

        let approval_id = diesel::insert_into(approvals::approvals)
            .values(insert)
            .returning(approvals::id)
            .get_result::<i32>(c)?;

        Ok(ApprovalState::Pending(approval_id))
    }
}