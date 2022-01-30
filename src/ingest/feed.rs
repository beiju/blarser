use rocket::info;
use crate::ingest::task::IngestState;

pub async fn ingest_feed(_db: IngestState, _start_at_time: &'static str) {
    info!("Started Feed ingest task");
}