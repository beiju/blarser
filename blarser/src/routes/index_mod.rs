use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use serde::Serialize;

use blarser::db::{BlarserDbConn, get_latest_ingest};
use crate::routes::ApiError;

#[rocket::get("/")]
pub async fn index(conn: BlarserDbConn) -> Result<Template, ApiError> {
    let ingest = conn.run(|c| {
        get_latest_ingest(c)
    }).await
        .map_err(|err: DieselError| ApiError::InternalError(err.to_string()))?;

    #[derive(Serialize)]
    struct IndexTemplateParams {
        ingest_started_at: String,
    }

    match ingest {
        None => { Ok(Template::render("no-ingests", ())) }
        Some(ingest) => {
            Ok(Template::render("index", IndexTemplateParams {
                ingest_started_at: ingest.started_at.format("%c").to_string(),
            }))
        }
    }
}
