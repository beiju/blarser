use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use chrono::{DateTime, Utc};
use diesel::{insert_into, RunQueryDsl};
use rocket::info;
use tokio::sync::{watch, oneshot};

use crate::db::BlarserDbConn;
use crate::ingest::chron::{init_chron, ingest_chron};
use crate::ingest::feed::ingest_feed;
use crate::schema;

const BLARSER_START: &str = "2021-12-06T15:00:00Z";

pub struct IngestTaskHolder {
    pub latest_ingest: Arc<StdMutex<Option<IngestTask>>>
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

pub struct ChronIngest {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
    pub send_chron_progress: watch::Sender<DateTime<Utc>>,
    pub receive_feed_progress: watch::Receiver<DateTime<Utc>>,
    pub pending_approvals: Arc<StdMutex<HashMap<i32, oneshot::Sender<bool>>>>,
}

pub struct FeedIngest {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
    pub send_feed_progress: watch::Sender<DateTime<Utc>>,
    pub receive_chron_progress: watch::Receiver<DateTime<Utc>>,
}

impl IngestTask {
    pub async fn new(feed_db: BlarserDbConn, chron_db: BlarserDbConn) -> IngestTask {
        info!("Starting ingest");

        let ingest_id: i32 = feed_db.run(|c| {
            use schema::ingests::dsl::*;
            insert_into(ingests).default_values().returning(id).get_result(c)
        }).await
            .expect("Failed to create new ingest record");

        let blarser_start = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse Blarser start time")
            .with_timezone(&Utc);

        let (send_feed_progress, receive_feed_progress) = watch::channel(blarser_start);
        let (send_chron_progress, receive_chron_progress) = watch::channel(blarser_start);

        let approvals = Arc::new(StdMutex::new(HashMap::new()));
        let feed_ingest = FeedIngest {
            ingest_id,
            db: feed_db,
            send_feed_progress,
            receive_chron_progress,
        };

        let mut chron_ingest = ChronIngest {
            ingest_id,
            db: chron_db,
            send_chron_progress,
            receive_feed_progress,
            pending_approvals: approvals.clone(),
        };

        let start_time_parsed = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse hard-coded Blarser start time")
            .with_timezone(&Utc);

        init_chron(&mut chron_ingest, BLARSER_START, start_time_parsed).await;

        tokio::spawn(ingest_chron(chron_ingest, BLARSER_START));
        tokio::spawn(ingest_feed(feed_ingest, BLARSER_START));

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