use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use serde::Serialize;

use blarser::db::{BlarserDbConn, get_latest_ingest, IngestLogAndApproval};
use crate::routes::ApiError;

#[rocket::get("/")]
pub async fn index(conn: BlarserDbConn) -> Result<Template, ApiError> {
    let (ingest, logs) = conn.run(|c| {
        let ingest = get_latest_ingest(c)?;
        let logs = vec![]; // TODO

        Ok((ingest, logs))
    }).await
        .map_err(|err: DieselError| ApiError::InternalError(err.to_string()))?;

    #[derive(Serialize)]
    struct IndexTemplateParams {
        ingest_started_at: String,
        events_parsed: i32,
        logs: Vec<IngestLogAndApproval>,
    }

    match ingest {
        None => { Ok(Template::render("no-ingests", ())) }
        Some(ingest) => {
            Ok(Template::render("index", IndexTemplateParams {
                ingest_started_at: ingest.started_at.format("%c").to_string(),
                events_parsed: ingest.events_parsed,
                logs,
            }))
        }
    }
}
