use rocket::fairing::AdHoc;
use rocket_dyn_templates::Template;
use blarser::ingest;
use blarser::db::{BlarserDbConn};
use routes::index;

mod routes;

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
