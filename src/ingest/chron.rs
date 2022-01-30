use rocket::info;
use crate::db::BlarserDbConn;

pub async fn ingest_chron(_db: BlarserDbConn) {
    info!("Started Chron ingest task");
}