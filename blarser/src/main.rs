#![feature(split_array)]

use rocket::fairing::{AdHoc, Fairing, Info, Kind};
use rocket::fs::{FileServer, relative};
use rocket::{Error, Request, Response};
use rocket::http::Header;
use rocket_dyn_templates::Template;
use blarser::ingest::{IngestTaskHolder, IngestTask};
use blarser::db::{BlarserDbConn};
use routes::{index, approvals, approve, debug, entity_debug_json, /*entities*/};

mod routes;
mod debug_routes;

pub struct CORS;

#[rocket::async_trait]
impl Fairing for CORS {
    fn info(&self) -> Info {
        Info {
            name: "Add CORS headers to responses",
            kind: Kind::Response
        }
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        response.set_header(Header::new("Access-Control-Allow-Origin", "*"));
        response.set_header(Header::new("Access-Control-Allow-Methods", "POST, GET, PATCH, OPTIONS"));
        response.set_header(Header::new("Access-Control-Allow-Headers", "*"));
        response.set_header(Header::new("Access-Control-Allow-Credentials", "true"));
    }
}


// Using main as an entry point instead of rocket::launch because CLion doesn't understand launch
#[rocket::main]
async fn main() -> Result<(), Error> {
    let _ = rocket::build()
        .mount("/public", FileServer::from(relative!("static")))
        .mount("/", rocket::routes![index, approvals, approve, debug, entity_debug_json, /*entities*/])
        .mount("/api/debug", debug_routes::routes())
        .attach(BlarserDbConn::fairing())
        .attach(Template::fairing())
        .attach(CORS)
        .manage(IngestTaskHolder::new())
        .attach(AdHoc::on_liftoff("Blarser Ingest", |rocket| Box::pin(async {
            let conn = BlarserDbConn::get_one(rocket).await.unwrap();
            let task_holder: &IngestTaskHolder = rocket.state().unwrap();

            let ingest_task = IngestTask::new(conn).await;
            let mut task_mut = task_holder.latest_ingest.lock().unwrap();
            *task_mut = Some(ingest_task);
        })))
        .launch().await?;
    Ok(())
}
