use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::schema::*;
use crate::state::EntityType;

#[derive(Insertable)]
#[diesel(table_name = approvals)]
pub struct NewApproval<'a> {
    pub entity_type: EntityType,
    pub entity_id: Uuid,
    pub perceived_at: DateTime<Utc>,
    pub message: &'a str,
}

pub enum ApprovalState {
    Pending(i32),
    Approved,
    Rejected,
}