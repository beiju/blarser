use rocket::fairing::AdHoc;
use rocket_dyn_templates::Template;
use serde::Serialize;
use blarser::ingest;
use blarser::db::BlarserDbConn;

#[derive(Serialize)]
struct Index {}

#[rocket::get("/")]
fn index() -> Template {
    Template::render("index", Index {})
}

// Using main as an entry point instead of rocket::launch because CLion doesn't understand launch
#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    // tokio::spawn(ingest::run(BlarserDbConn));

    rocket::build()
        .mount("/", rocket::routes![index])
        .attach(BlarserDbConn::fairing())
        .attach(Template::fairing())
        .attach(AdHoc::on_liftoff("Start Ingest", |rocket| Box::pin(async {
            ingest::run(BlarserDbConn::get_one(rocket).await.unwrap()).await.unwrap();
            ()
        })))
        .launch().await
}
