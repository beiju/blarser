use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, QueryResult, RunQueryDsl};
use rocket::info;
use core::default::Default;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use uuid::Uuid;

use crate::db::BlarserDbConn;
use crate::ingest::run_ingest;
use crate::ingest::state::StateGraph;
use crate::schema;
use crate::state::{ApprovalState, EntityType, StateInterface};

// Doing 15:31 to skip a trivial change that just changes the milliseconds of every date in `sim`,
// I'm guessing due to a sim restart or something
const BLARSER_START: &str = "2021-03-01T15:31:00Z";

pub struct IngestTaskHolder {
    pub latest_ingest: Arc<StdMutex<Option<IngestTask>>>,
}

impl IngestTaskHolder {
    pub fn new() -> Self {
        Self {
            latest_ingest: Arc::new(StdMutex::new(None))
        }
    }

    pub fn latest_ingest_id(&self) -> Option<i32> {
        let lock = self.latest_ingest.lock().unwrap();
        lock.as_ref().map(|ingest| ingest.ingest_id)
    }

    pub fn notify_approval(&self, id: i32, result: bool) {
        let lock = self.latest_ingest.lock().unwrap();
        if let Some(task) = &*lock {
            task.notify_approval(id, result)
        }
    }
}

impl Default for IngestTaskHolder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct IngestTask {
    ingest_id: i32,
    pending_approvals: Arc<StdMutex<HashMap<i32, oneshot::Sender<bool>>>>,
    pub debug_history: GraphDebugHistorySync,
}

impl IngestTask {
    pub async fn new(conn: BlarserDbConn) -> IngestTask {
        info!("Starting ingest");

        let ingest_id: i32 = conn.run(|c| {
            use diesel::dsl::*;
            use schema::ingests::dsl::*;

            // Delete all except latest ingest
            let latest_ingest = ingests
                .select(id)
                .order(started_at.desc())
                .limit(1)
                .get_result::<i32>(c)
                .optional()?;

            if let Some(latest_ingest) = latest_ingest {
                delete(ingests.filter(id.ne(latest_ingest))).execute(c)?;
            }

            insert_into(ingests).default_values().returning(id).get_result(c)
        }).await
            .expect("Failed to create new ingest record");

        let start_time_parsed = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse hard-coded Blarser start time")
            .with_timezone(&Utc);

        let approvals = Arc::new(StdMutex::new(HashMap::new()));
        let ingest = Ingest::new(ingest_id, conn);
        let debug_history = ingest.debug_history.clone();

        tokio::spawn(run_ingest(ingest, start_time_parsed));

        IngestTask {
            ingest_id,
            pending_approvals: approvals,
            debug_history,
        }
    }

    pub fn notify_approval(&self, id: i32, result: bool) {
        let mut pending_approvals = self.pending_approvals.lock().unwrap();
        if let Some(sender) = pending_approvals.remove(&id) {
            sender.send(result)
                .expect("Approval channel was unexpectedly closed");
        }
    }
}

pub struct DebugHistoryVersion {
    pub event_human_name: String,
    pub value: serde_json::Value,
}

pub struct DebugHistoryItem {
    pub entity_human_name: String,
    pub time: DateTime<Utc>,
    pub versions: Vec<DebugHistoryVersion>,
}

pub type GraphDebugHistory = HashMap<(EntityType, Uuid), DebugHistoryItem>;
pub type GraphDebugHistorySync = Arc<TokioMutex<GraphDebugHistory>>;

pub struct Ingest {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
    pub pending_approvals: Arc<StdMutex<HashMap<i32, oneshot::Sender<bool>>>>,
    pub state: Arc<StdMutex<StateGraph>>,
    pub debug_history: GraphDebugHistorySync,
}

impl Ingest {
    pub fn new(ingest_id: i32, db: BlarserDbConn) -> Self {
        Self {
            ingest_id,
            db,
            pending_approvals: Arc::new(StdMutex::new(Default::default())),
            state: Arc::new(StdMutex::new(StateGraph::new())),
            debug_history: Arc::new(TokioMutex::new(Default::default())),
        }
    }

    pub async fn run<F, R>(&self, f: F) -> R
        where F: FnOnce(StateInterface) -> R + Send + 'static,
              R: Send + 'static {
        let ingest_id = self.ingest_id;
        self.db.run(move |c| {
            f(StateInterface::new(c, ingest_id))
        }).await
    }

    pub async fn get_approval(&mut self, entity_type: EntityType, entity_id: Uuid, perceived_at: DateTime<Utc>, message: String) -> QueryResult<bool> {
        let result = self.run(move |mut state| {
            state.upsert_approval(entity_type, entity_id, perceived_at, &message)
        }).await?;

        match result {
            ApprovalState::Pending(id) => {
                let (send, recv) = oneshot::channel();
                // New scope to make sure pending_approvals is unlocked before waiting on the channel
                {
                    let mut pending_approvals = self.pending_approvals.lock().unwrap();
                    pending_approvals.insert(id, send);
                }
                let result = recv.await
                    .expect("Pending approval channel dropped unexpectedly");

                Ok(result)
            }
            ApprovalState::Approved => { Ok(true) }
            ApprovalState::Rejected => { Ok(false) }
        }
    }
}