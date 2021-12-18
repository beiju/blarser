use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc::Sender};
use log::error;

use crate::db::BlarserDbConn;
use crate::ingest::ingest;
use crate::ingest::log::IngestLogger;

type CallbackRegistry = Arc<Mutex<HashMap<i32, Sender<()>>>>;

#[derive(Clone)]
pub struct IngestTask {
    registry: CallbackRegistry,
}

impl IngestTask {
    pub fn register_callback(&self, approval_id: i32, sender: Sender<()>) {
        self.registry.lock().unwrap().insert(approval_id, sender);
    }

    pub fn unregister_callback(&self, approval_id: i32) {
        self.registry.lock().unwrap().remove(&approval_id);
    }

    pub fn notify_callback(&self, approval_id: i32) {
        let sender = self.registry.lock().unwrap().remove(&approval_id);
        if let Some(sender) = sender {
            sender.send(()).unwrap();
        }
    }
}

impl IngestTask {
    pub fn new() -> IngestTask {
        IngestTask { registry: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn start(&self, db: BlarserDbConn) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            db.run(|c| {
                let log = IngestLogger::new(c, self_clone).unwrap();
                let result = ingest::run(log);
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{:?}", e);
                    }
                }
            }).await
        });
    }
}