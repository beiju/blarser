use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use serde::Serialize;

use blarser::db::{BlarserDbConn, get_latest_ingest, get_logs_for, IngestLog};

#[derive(rocket::Responder)]
pub enum ServerError {
    #[response(status = 500)]
    InternalError(String)
}

#[rocket::get("/")]
pub async fn index(conn: BlarserDbConn) -> Result<Template, ServerError> {
    let (ingest, logs) = conn.run(|c| {
        let ingest = get_latest_ingest(&c)?;
        let logs = match &ingest {
            Some(ingest) => get_logs_for(ingest, &c)?,
            None => vec![]
        };

        Ok((ingest, logs))
    }).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    #[derive(Serialize)]
    struct IndexTemplateParams {
        ingest_started_at: String,
        logs: Vec<IngestLog>,
    }

    match ingest {
        None => { Ok(Template::render("index-no-ingests", ())) }
        Some(ingest) => {
            Ok(Template::render("index", IndexTemplateParams {
                ingest_started_at: ingest.started_at.format("%c").to_string(),
                logs
            }))
        }
    }
}
