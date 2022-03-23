use chrono::{DateTime, Utc};

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
    Approved,
    Rejected,
}