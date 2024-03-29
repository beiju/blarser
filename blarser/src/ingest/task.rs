use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, QueryResult, RunQueryDsl};
use rocket::info;
use core::default::Default;
use petgraph::stable_graph::NodeIndex;
use serde::Serialize;
use tokio::sync::{oneshot, mpsc, Mutex as TokioMutex};
use uuid::Uuid;

use crate::db::BlarserDbConn;
use crate::ingest::run_ingest;
use crate::ingest::state::{AddedReason, StateGraph};
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
    pub pause_requester: Arc<TokioMutex<mpsc::Sender<oneshot::Receiver<()>>>>,
    pub resumer: Option<oneshot::Sender<()>>,
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
        let (pause_requester, pause_requests) = mpsc::channel(10);
        let ingest = Ingest::new(ingest_id, conn, pause_requests);
        let debug_history = ingest.debug_history.clone();

        tokio::spawn(run_ingest(ingest, start_time_parsed));

        IngestTask {
            ingest_id,
            pending_approvals: approvals,
            debug_history,
            pause_requester: Arc::new(TokioMutex::new(pause_requester)),
            resumer: None,
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

#[derive(Debug, Serialize, Clone)]
pub struct DebugTreeNode {
    pub description: String,
    pub is_ambiguous: bool,
    pub created_at: DateTime<Utc>,
    pub observed_at: Option<DateTime<Utc>>,
    pub added_reason: AddedReason,
    pub json: serde_json::Value,
    pub order: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct DebugTree {
    pub generations: Vec<HashSet<NodeIndex>>,
    pub edges: HashMap<NodeIndex, Vec<NodeIndex>>,
    pub data: HashMap<NodeIndex, DebugTreeNode>,
    pub roots: Vec<NodeIndex>,
    pub leafs: Vec<NodeIndex>,
}

#[derive(Debug, Serialize)]
pub struct DebugHistoryVersion {
    pub event_human_name: String,
    pub time: DateTime<Utc>,
    pub tree: DebugTree,
    pub queued_for_update: Option<HashSet<NodeIndex>>,
    pub currently_updating: Option<NodeIndex>,
    pub queued_for_delete: Option<HashSet<NodeIndex>>,
}

pub struct DebugHistoryItem {
    pub entity_human_name: String,
    pub versions: Vec<DebugHistoryVersion>,
}

pub struct GraphDebugHistory {
    disabled: bool,
    inner: HashMap<(EntityType, Uuid), DebugHistoryItem>,
}

impl GraphDebugHistory {
    pub fn new(disabled: bool) -> Self {
        Self {
            disabled,
            inner: Default::default(),
        }
    }

    pub fn push_item(&mut self, key: (EntityType, Uuid), item: DebugHistoryItem) {
        if self.disabled { return }
        self.inner.insert(key, item);
    }

    // Shortcut for push_version
    pub fn push(&mut self, key: &(EntityType, Uuid), version: DebugHistoryVersion) {
        if self.disabled { return }
        self.inner.get_mut(key).unwrap().versions.push(version);
    }

    pub fn iter(&self) -> impl Iterator<Item=(&(EntityType, Uuid), &DebugHistoryItem)> {
        self.inner.iter()
    }

    pub fn get(&self, key: &(EntityType, Uuid)) -> Option<&DebugHistoryItem> {
        self.inner.get(key)
    }
}

pub type GraphDebugHistorySync = Arc<TokioMutex<GraphDebugHistory>>;

pub struct Ingest {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
    pub pending_approvals: Arc<StdMutex<HashMap<i32, oneshot::Sender<bool>>>>,
    pub state: Arc<StdMutex<StateGraph>>,
    pub debug_history: GraphDebugHistorySync,
    pub pause_request: mpsc::Receiver<oneshot::Receiver<()>>,
}

impl Ingest {
    pub fn new(ingest_id: i32, db: BlarserDbConn, pause_request: mpsc::Receiver<oneshot::Receiver<()>>) -> Self {
        Self {
            ingest_id,
            db,
            pending_approvals: Arc::new(StdMutex::new(Default::default())),
            state: Arc::new(StdMutex::new(StateGraph::new())),
            debug_history: Arc::new(TokioMutex::new(GraphDebugHistory::new(false))),
            pause_request,
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