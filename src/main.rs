use rocket::fairing::AdHoc;
use rocket::fs::{FileServer, relative};
use rocket_dyn_templates::Template;
use blarser::ingest::IngestTask;
use blarser::db::{BlarserDbConn};
use routes::{index, approvals, approve};

mod routes;

// Using main as an entry point instead of rocket::launch because CLion doesn't understand launch
#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    rocket::build()
        .mount("/public", FileServer::from(relative!("static")))
        .mount("/", rocket::routes![index, approvals, approve])
        .attach(BlarserDbConn::fairing())
        .attach(Template::fairing())
        .manage(IngestTask::new())
        .attach(AdHoc::on_liftoff("Blarser Ingest", |rocket| Box::pin(async {
            let feed_conn = BlarserDbConn::get_one(rocket).await.unwrap();
            let chron_conn = BlarserDbConn::get_one(rocket).await.unwrap();
            let ingest_task: &IngestTask = rocket.state().unwrap();
            ingest_task.start(feed_conn, chron_conn);
        })))
        .launch().await
}
