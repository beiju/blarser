use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex as StdMutex, Mutex};
use chrono::{DateTime, Duration, Utc};
use diesel::{ExpressionMethods, OptionalExtension, PgConnection, QueryDsl, QueryResult, RunQueryDsl};
use diesel::result::Error;
use multimap::MultiMap;
use rocket::info;
use tokio::sync::{watch, oneshot};
use uuid::Uuid;
use crate::api::EventuallyEvent;

use crate::db::BlarserDbConn;
use crate::ingest::chron::{init_chron, run_ingest};
use crate::schema;
use crate::state::{ApprovalState, StateInterface};

const BLARSER_START: &str = "2021-12-06T15:00:00Z";

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

        let approvals = Arc::new(StdMutex::new(HashMap::new()));
        let ingest = Ingest::new(ingest_id, conn);

        let start_time_parsed = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse hard-coded Blarser start time")
            .with_timezone(&Utc);

        init_chron(&ingest, BLARSER_START, start_time_parsed).await;

        tokio::spawn(run_ingest(ingest, BLARSER_START, start_time_parsed));

        IngestTask {
            ingest_id,
            pending_approvals: approvals,
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

pub struct Ingest {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
    pub pending_approvals: Arc<StdMutex<HashMap<i32, oneshot::Sender<bool>>>>,
}

impl Ingest {
    pub fn new(ingest_id: i32, db: BlarserDbConn) -> Self {
        Self {
            ingest_id,
            db,
            pending_approvals: Arc::new(Mutex::new(Default::default())),
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

    pub async fn get_approval(&self, entity_type: &'static str, entity_id: Uuid, perceived_at: DateTime<Utc>, message: String) -> QueryResult<bool> {
        let result = self.run(move |state| {
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