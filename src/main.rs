use rocket_sync_db_pools::{database, postgres};
use rocket_dyn_templates::Template;
use serde::Serialize;
use blarser::ingest;

#[database("blarser")]
struct BlarserDbConn(postgres::Client);

#[derive(Serialize)]
struct Index {}

#[rocket::get("/")]
fn index() -> Template {
    Template::render("index", Index {})
}

// Using main as an entry point instead of rocket::launch because CLion doesn't understand launch
#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    tokio::spawn(ingest::run());

    rocket::build()
        .mount("/", rocket::routes![index])
        .attach(BlarserDbConn::fairing())
        .attach(Template::fairing())
        .launch().await
}
