use std::sync::{Mutex};

use tokio::task::JoinHandle;

use crate::db::BlarserDbConn;
use crate::ingest::chron::ingest_chron;
use crate::ingest::feed::ingest_feed;

pub struct IngestTask {
    feed_task: Mutex<Option<JoinHandle<()>>>,
    chron_task: Mutex<Option<JoinHandle<()>>>,
}

impl IngestTask {
    pub fn new() -> IngestTask {
        IngestTask {
            feed_task: Mutex::new(None),
            chron_task: Mutex::new(None)
        }
    }

    pub fn start(&self, feed_db: BlarserDbConn, chron_db: BlarserDbConn) {
        *self.feed_task.lock().unwrap() = Some(tokio::spawn(ingest_feed(feed_db)));
        *self.chron_task.lock().unwrap() = Some(tokio::spawn(ingest_chron(chron_db)));
    }
}

impl Default for IngestTask {
    fn default() -> Self {
        IngestTask::new()
    }
}