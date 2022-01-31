use std::sync::Mutex;
use chrono::{DateTime, Utc};
use diesel::{insert_into, RunQueryDsl};
use rocket::info;
use tokio::sync::watch;

use tokio::task::JoinHandle;

use crate::db::BlarserDbConn;
use crate::ingest::chron::ingest_chron;
use crate::ingest::feed::ingest_feed;
use crate::schema;

const BLARSER_START: &str = "2021-12-06T15:00:00Z";

pub struct IngestTask {
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
            feed_task: Mutex::new(None),
            chron_task: Mutex::new(None)
        }
    }

    pub async fn start(&self, feed_db: BlarserDbConn, chron_db: BlarserDbConn) {
        info!("Starting ingest");

        let ingest_id: i32 = feed_db.run(|c| {
            use schema::ingests::dsl::*;
            insert_into(ingests).default_values().returning(id).get_result(c)
        }).await.expect("Failed to create new ingest record");


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

        let chron_ingest = IngestState {
            ingest_id,
            db: chron_db,
            notify_progress: send_chron_progress,
            receive_progress: recv_feed_progress,
        };

        *self.chron_task.lock().unwrap() = Some(tokio::spawn(ingest_chron(chron_ingest, BLARSER_START)));
        *self.feed_task.lock().unwrap() = Some(tokio::spawn(ingest_feed(feed_ingest, BLARSER_START)));
    }
}

impl Default for IngestTask {
    fn default() -> Self {
        IngestTask::new()
    }
}