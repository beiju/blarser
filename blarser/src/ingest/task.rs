use std::sync::Mutex;
use chrono::{DateTime, Utc};
use diesel::{insert_into, RunQueryDsl};
use rocket::info;
use tokio::sync::{OnceCell, watch};

use tokio::task::JoinHandle;

use crate::db::BlarserDbConn;
use crate::ingest::chron::{init_chron, ingest_chron};
use crate::ingest::feed::ingest_feed;
use crate::schema;

const BLARSER_START: &str = "2021-12-06T15:00:00Z";

pub struct IngestTask {
    latest_ingest_id: OnceCell<i32>,
    feed_task: Mutex<Option<JoinHandle<()>>>,
    chron_task: Mutex<Option<JoinHandle<()>>>,
}

pub struct IngestState {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
    pub notify_progress: watch::Sender<DateTime<Utc>>,
    pub receive_progress: watch::Receiver<DateTime<Utc>>,
}

impl IngestTask {
    pub fn new() -> IngestTask {
        IngestTask {
            latest_ingest_id: OnceCell::new(),
            feed_task: Mutex::new(None),
            chron_task: Mutex::new(None)
        }
    }

    pub fn latest_ingest(&self) -> Option<i32> {
        self.latest_ingest_id.get().cloned()
    }

    pub async fn start(&self, feed_db: BlarserDbConn, chron_db: BlarserDbConn) {
        info!("Starting ingest");

        let ingest_id: i32 = feed_db.run(|c| {
            use schema::ingests::dsl::*;
            insert_into(ingests).default_values().returning(id).get_result(c)
        }).await.expect("Failed to create new ingest record");

        self.latest_ingest_id.set(ingest_id)
            .expect("Error saving latest ingest id");

        let blarser_start = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse Blarser start time")
            .with_timezone(&Utc);

        let (send_feed_progress, recv_feed_progress) = watch::channel(blarser_start);
        let (send_chron_progress, recv_chron_progress) = watch::channel(blarser_start);

        let feed_ingest = IngestState {
            ingest_id,
            db: feed_db,
            notify_progress: send_feed_progress,
            receive_progress: recv_chron_progress,
        };

        let mut chron_ingest = IngestState {
            ingest_id,
            db: chron_db,
            notify_progress: send_chron_progress,
            receive_progress: recv_feed_progress,
        };

        let start_time_parsed = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse hard-coded Blarser start time")
            .with_timezone(&Utc);

        init_chron(&mut chron_ingest, BLARSER_START, start_time_parsed).await;

        *self.chron_task.lock().unwrap() = Some(tokio::spawn(ingest_chron(chron_ingest, start_time_parsed)));
        *self.feed_task.lock().unwrap() = Some(tokio::spawn(ingest_feed(feed_ingest, BLARSER_START)));
    }
}

impl Default for IngestTask {
    fn default() -> Self {
        IngestTask::new()
    }
}