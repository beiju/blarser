use rocket::fairing::AdHoc;
use rocket_dyn_templates::Template;
use serde::Serialize;
use blarser::ingest;
use blarser::db::{BlarserDbConn, get_latest_ingest, Ingest};

#[derive(Serialize)]
struct Index {
    ingest_started_at: String,
}

#[derive(rocket::Responder)]
enum ServerError {
    #[response(status = 500)]
    InternalError(String)
}

#[rocket::get("/")]
async fn index(conn: BlarserDbConn) -> Result<Template, ServerError> {
    let ingest = conn.run(|c|
        get_latest_ingest(&c)
    ).await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    match ingest {
        None => { Ok(Template::render("index-no-ingests", ())) }
        Some(ingest) => {
            Ok(Template::render("index", Index {
                ingest_started_at: ingest.started_at.format("%c").to_string()
            }))
        }
    }
}

// Using main as an entry point instead of rocket::launch because CLion doesn't understand launch
#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    // tokio::spawn(ingest::run(BlarserDbConn));

    rocket::build()
        .mount("/", rocket::routes![index])
        .attach(BlarserDbConn::fairing())
        .attach(Template::fairing())
        // .attach(AdHoc::on_liftoff("Blarser Ingest", |rocket| Box::pin(async {
        //     let db = BlarserDbConn::get_one(rocket).await.unwrap();
        //     ingest::run(db).await.unwrap();
        //     ()
        // })))
        .launch().await
}
