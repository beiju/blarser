use rocket::info;
use crate::db::BlarserDbConn;

pub async fn ingest_feed(_db: BlarserDbConn) {
    info!("Started Feed ingest task");
}