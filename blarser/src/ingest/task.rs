use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, Mutex};
use chrono::{DateTime, Duration, Utc};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl};
use rocket::info;
use tokio::sync::{watch, oneshot};

use crate::db::BlarserDbConn;
use crate::ingest::chron::{init_chron, ingest_chron};
use crate::ingest::feed::ingest_feed;
use crate::schema;
use crate::state::StateInterface;

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

struct IngestImpl {
    pub ingest_id: i32,
    pub db: BlarserDbConn,
}

impl IngestImpl {
    #[allow(dead_code)]
    pub async fn run<F, R>(&self, f: F) -> R
        where F: FnOnce(StateInterface) -> R + Send + 'static,
              R: Send + 'static {
        let ingest_id = self.ingest_id;
        self.db.run(move |c| {
            f(StateInterface::new(c, ingest_id))
        }).await
    }

    pub async fn run_transaction<F, T, E>(&self, f: F) -> Result<T, E>
        where F: FnOnce(StateInterface) -> Result<T, E> + Send + 'static,
              T: Send + 'static,
              E: From<diesel::result::Error> + Send + 'static {
        let ingest_id = self.ingest_id;
        self.db.run(move |c| {
            c.build_transaction()
                .serializable()
                .run(|| {
                    f(StateInterface::new(c, ingest_id))
                })
        }).await
    }
}

pub struct ChronIngest {
    ingest_impl: IngestImpl,
    pub send_chron_progress: watch::Sender<DateTime<Utc>>,
    pub receive_feed_progress: watch::Receiver<DateTime<Utc>>,
    pub pending_approvals: Arc<StdMutex<HashMap<i32, oneshot::Sender<bool>>>>,
}

impl ChronIngest {
    pub fn new(
        db: BlarserDbConn,
        ingest_id: i32,
        send_chron_progress: watch::Sender<DateTime<Utc>>,
        receive_feed_progress: watch::Receiver<DateTime<Utc>>
    ) -> Self {
        Self {
            ingest_impl: IngestImpl {
                ingest_id,
                db
            },
            send_chron_progress,
            receive_feed_progress,
            pending_approvals: Arc::new(Mutex::new(Default::default()))
        }
    }

    //noinspection DuplicatedCode
    pub async fn run<F, R>(&self, f: F) -> R
        where F: FnOnce(StateInterface) -> R + Send + 'static,
              R: Send + 'static {
        self.ingest_impl.run(f).await
    }

    //noinspection DuplicatedCode
    pub async fn run_transaction<F, T, E>(&self, f: F) -> Result<T, E>
        where F: FnOnce(StateInterface) -> Result<T, E> + Send + 'static,
              T: Send + 'static,
              E: From<diesel::result::Error> + Send + 'static {
        self.ingest_impl.run_transaction(f).await
    }

    pub async fn wait_for_feed_ingest(&mut self, wait_until_time: DateTime<Utc>) {
        self.send_chron_progress.send(wait_until_time)
            .expect("Error communicating with Chronicler ingest");
        // info!("Chron ingest sent {} as requested time", wait_until_time);

        loop {
            let feed_time = *self.receive_feed_progress.borrow();
            if wait_until_time < feed_time {
                break;
            }
            // info!("Chronicler ingest waiting for Eventually ingest to catch up (at {} and we need {}, difference of {}s)",
            // feed_time, wait_until_time, (wait_until_time - feed_time).num_seconds());
            self.receive_feed_progress.changed().await
                .expect("Error communicating with Eventually ingest");
        }
    }

}

pub struct FeedIngest {
    ingest_impl: IngestImpl,
    send_feed_progress: watch::Sender<DateTime<Utc>>,
    receive_chron_progress: watch::Receiver<DateTime<Utc>>,
}

impl FeedIngest {
    pub fn new(
        db: BlarserDbConn,
        ingest_id: i32,
        send_feed_progress: watch::Sender<DateTime<Utc>>,
        receive_chron_progress: watch::Receiver<DateTime<Utc>>
    ) -> Self {
        Self {
            ingest_impl: IngestImpl {
                ingest_id,
                db
            },
            send_feed_progress,
            receive_chron_progress
        }
    }

    //noinspection DuplicatedCode
    pub async fn run<F, R>(&self, f: F) -> R
        where F: FnOnce(StateInterface) -> R + Send + 'static,
              R: Send + 'static {
        self.ingest_impl.run(f).await
    }

    //noinspection DuplicatedCode
    pub async fn run_transaction<F, T, E>(&self, f: F) -> Result<T, E>
        where F: FnOnce(StateInterface) -> Result<T, E> + Send + 'static,
              T: Send + 'static,
              E: From<diesel::result::Error> + Send + 'static {
        self.ingest_impl.run_transaction(f).await
    }

    pub async fn wait_for_chron_ingest(&mut self, feed_event_time: DateTime<Utc>) {
        self.send_feed_progress.send(feed_event_time)
            .expect("Error communicating with Chronicler ingest");
        // info!("Feed ingest sent progress {}", feed_event_time);

        loop {
            let chron_requests_time = *self.receive_chron_progress.borrow();
            let stop_at = chron_requests_time + Duration::seconds(1);
            if feed_event_time < stop_at {
                break;
            }
            // info!("Eventually ingest waiting for Chronicler ingest to catch up (at {} and we are at {}, {}s ahead)",
            //         chron_requests_time, feed_event_time, (feed_event_time - chron_requests_time).num_seconds());
            self.receive_chron_progress.changed().await
                .expect("Error communicating with Chronicler ingest");
        }
    }
}


impl IngestTask {
    pub async fn new(feed_db: BlarserDbConn, chron_db: BlarserDbConn) -> IngestTask {
        info!("Starting ingest");

        let ingest_id: i32 = feed_db.run(|c| {
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

        let blarser_start = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse Blarser start time")
            .with_timezone(&Utc);

        let (send_feed_progress, receive_feed_progress) = watch::channel(blarser_start);
        let (send_chron_progress, receive_chron_progress) = watch::channel(blarser_start);

        let approvals = Arc::new(StdMutex::new(HashMap::new()));
        let feed_ingest = FeedIngest::new(feed_db, ingest_id, send_feed_progress, receive_chron_progress);
        let chron_ingest = ChronIngest::new(chron_db, ingest_id, send_chron_progress, receive_feed_progress);

        let start_time_parsed = DateTime::parse_from_rfc3339(BLARSER_START)
            .expect("Couldn't parse hard-coded Blarser start time")
            .with_timezone(&Utc);

        init_chron(&chron_ingest, BLARSER_START, start_time_parsed).await;

        tokio::spawn(ingest_chron(chron_ingest, BLARSER_START));
        tokio::spawn(ingest_feed(feed_ingest, BLARSER_START, start_time_parsed));

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