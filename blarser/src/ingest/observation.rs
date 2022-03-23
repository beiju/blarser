use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use thiserror::Error;

use crate::api::ChroniclerItem;
use crate::entity::{AnyEntityRaw, EntityParseError, EntityRaw};
use crate::with_any_entity_raw;


#[derive(Clone)]
pub struct Observation {
    // TODO Reorganize so this isn't pub
    pub entity_raw: AnyEntityRaw,
    pub perceived_at: DateTime<Utc>,
}

impl Observation {
    pub fn from_chron(entity_type: &str, item: ChroniclerItem) -> Result<Self, EntityParseError> {
        let entity_raw = AnyEntityRaw::from_json(entity_type, item.data)?;

        Ok(Observation {
            entity_raw,
            perceived_at: item.valid_from,
        })
    }

    pub fn earliest_time(&self) -> DateTime<Utc> {
        with_any_entity_raw!(&self.entity_raw, inner => inner.earliest_time(self.perceived_at))
    }

    pub fn latest_time(&self) -> DateTime<Utc> {
        with_any_entity_raw!(&self.entity_raw, inner => inner.latest_time(self.perceived_at))
    }

    // pub fn do_ingest(self, ingest: &mut ChronIngest) {
    //     let ingest_id = ingest.ingest_id;
    //     let (approval, this) = ingest.db.run(move |c| {
    //         let approval_result = c.build_transaction()
    //             .serializable()
    //             .run(|| {
    //                 let conflicts = self.do_ingest_internal(c, ingest_id, false);
    //
    //                 // Round-trip through the Result machinery to get diesel to cancel the transaction
    //                 match conflicts {
    //                     None => { Ok(()) }
    //                     Some(c) => { Err(IngestError::NeedsApproval(c)) }
    //                 }
    //             });
    //
    //         if let Err(IngestError::NeedsApproval(approval)) = approval_result {
    //             (Some(approval), self)
    //         } else {
    //             approval_result.expect("Unexpected database error in chronicler ingest");
    //             (None, self)
    //         }
    //     }).await;
    //
    //     if let Some(conflicts) = approval {
    //         let entity_type = this.entity_raw.entity_type();
    //         let entity_id = this.entity_raw.entity_id();
    //         let entity_time = this.perceived_at;
    //
    //         // TODO Make a fun html debug view from conflicts info
    //         let message = conflicts.into_iter()
    //             .map(|(_, reason)| {
    //                 // TODO Print the info about which version the conflict is from, not about which
    //                 //   observation we tried to apply
    //                 format!("Can't apply observation to {} {} at {}:\n{}",
    //                         entity_type, entity_id, entity_time, reason)
    //             })
    //             .join("\n");
    //         let approval = ingest.db.run(move |c| {
    //             get_approval(c, entity_type, entity_id, entity_time, &message)
    //         }).await
    //             .expect("Error saving approval to db");
    //
    //         let approved = match approval {
    //             ApprovalState::Pending(approval_id) => {
    //                 let (send, recv) = oneshot::channel();
    //                 {
    //                     let mut pending_approvals = ingest.pending_approvals.lock().unwrap();
    //                     pending_approvals.insert(approval_id, send);
    //                 }
    //                 recv.await
    //                     .expect("Channel closed while awaiting approval")
    //             }
    //             ApprovalState::Approved(_) => { true }
    //             ApprovalState::Rejected => { false }
    //         };
    //
    //         if approved {
    //             ingest.db.run(move |c| {
    //                 c.transaction(|| {
    //                     let conflicts = this.do_ingest_internal(c, ingest_id, true);
    //
    //                     assert!(conflicts.is_none(), "Generated conflicts even with force=true");
    //                     Ok::<_, diesel::result::Error>(())
    //                 })
    //             }).await.unwrap();
    //         } else {
    //             panic!("Approval rejected")
    //         }
    //     }
    // }

    // fn do_ingest_internal(&self, c: &PgConnection, ingest_id: i32, force: bool) -> Option<Vec<(i32, String)>> {
    //     info!("Placing {} {} between {} and {}", self.entity_raw.entity_type(), self.entity_raw.entity_id(), self.earliest_time(), self.latest_time());

        // let (events, generations) = get_entity_update_tree(c, ingest_id, self.entity_raw.entity_type(), self.entity_raw.entity_id(), self.earliest_time())
        //     .expect("Error getting events for Chronicler ingest");
        //
        // if self.entity_id.to_string() == "781feeac-f948-43af-beee-14fa1328db76" && self.earliest_time.to_string() == "2021-12-06 16:00:10.303 UTC" {
        //     info!("BREAK");
        // }
        //
        // let mut to_terminate = None;
        //
        // let mut prev_generation = Vec::new();
        // let mut version_conflicts = Some(Vec::new());
        // for (event, versions) in events.into_iter().zip(generations) {
        //     let mut new_generation = MergedSuccessors::new();
        //
        //     if event.event_time <= self.latest_time {
        //         to_terminate = Some(versions.iter().map(|(v, _)| v.id).collect());
        //         observe_generation(&mut new_generation, &mut version_conflicts, versions, &self.entity_raw, self.perceived_at);
        //     }
        //
        //     advance_generation(c, ingest_id, &mut new_generation, event, prev_generation);
        //
        //     prev_generation = save_versions(c, new_generation.into_inner())
        //         .expect("Error saving updated versions");
        // }
        //
        // if let Some(to_terminate) = to_terminate {
        //     terminate_versions(c, to_terminate, format!("Failed to apply observation at {}", self.perceived_at))
        //         .expect("Failed to terminate versions");
        // }
        //
        // if version_conflicts.is_some() {
        //     info!("Conflicts!");
        // }
        //
        // version_conflicts

    //     todo!()
    // }
}

impl Eq for Observation {}

impl PartialEq<Self> for Observation {
    fn eq(&self, other: &Self) -> bool {
        self.latest_time().eq(&other.latest_time())
    }
}

impl PartialOrd<Self> for Observation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.latest_time().partial_cmp(&other.latest_time())
    }
}

impl Ord for Observation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.latest_time().cmp(&other.latest_time())
    }
}

#[derive(Debug, Error)]
enum IngestError {
    #[error("Needs approval: {0:?}")]
    NeedsApproval(Vec<(i32, String)>),

    #[error(transparent)]
    DieselError(#[from] diesel::result::Error),
}

