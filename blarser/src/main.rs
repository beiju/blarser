#![feature(split_array)]

use rocket::fairing::AdHoc;
use rocket::fs::{FileServer, relative};
use rocket_dyn_templates::Template;
use blarser::ingest::{IngestTaskHolder, IngestTask};
use blarser::db::{BlarserDbConn};
use routes::{index, approvals, approve, /*debug, entity_debug_json, entities*/};

mod routes;

// Using main as an entry point instead of rocket::launch because CLion doesn't understand launch
#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    rocket::build()
        .mount("/public", FileServer::from(relative!("static")))
        .mount("/", rocket::routes![index, approvals, approve, /*debug, entity_debug_json, entities*/])
        .attach(BlarserDbConn::fairing())
        .attach(Template::fairing())
        .manage(IngestTaskHolder::new())
        .attach(AdHoc::on_liftoff("Blarser Ingest", |rocket| Box::pin(async {
            let feed_conn = BlarserDbConn::get_one(rocket).await.unwrap();
            let chron_conn = BlarserDbConn::get_one(rocket).await.unwrap();
            let task_holder: &IngestTaskHolder = rocket.state().unwrap();

            let ingest_task = IngestTask::new(feed_conn, chron_conn).await;
            let mut task_mut = task_holder.latest_ingest.lock().unwrap();
            *task_mut = Some(ingest_task);
        })))
        .launch().await
}
