use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use serde::Serialize;
use rocket::form::{Form, FromForm};
use rocket::response::Redirect;
use rocket::{State, uri};


use blarser::db::{BlarserDbConn, get_pending_approvals_for, get_latest_ingest, get_logs_for, IngestApproval, IngestLog, set_approval};
use blarser::ingest::IngestTask;

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
        None => { Ok(Template::render("no-ingests", ())) }
        Some(ingest) => {
            Ok(Template::render("index", IndexTemplateParams {
                ingest_started_at: ingest.started_at.format("%c").to_string(),
                logs,
            }))
        }
    }
}


#[rocket::get("/approvals")]
pub async fn approvals(conn: BlarserDbConn) -> Result<Template, ServerError> {
    let (ingest, approvals) = conn.run(|c| {
        let ingest = get_latest_ingest(&c)?;
        let approvals = match &ingest {
            Some(ingest) => get_pending_approvals_for(ingest, &c)?,
            None => vec![]
        };

        Ok((ingest, approvals))
    }).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    #[derive(Serialize)]
    struct ApprovalTemplateParams {
        ingest_id: i32,
        ingest_started_at: String,
        approvals: Vec<IngestApproval>,
    }

    match ingest {
        None => { Ok(Template::render("no-ingests", ())) }
        Some(ingest) => {
            Ok(Template::render("approvals", ApprovalTemplateParams {
                ingest_id: ingest.id,
                ingest_started_at: ingest.started_at.format("%c").to_string(),
                approvals,
            }))
        }
    }
}


#[derive(FromForm)]
pub struct Approval {
    approval_id: i32,
    message: String,
    approved: bool,
}

#[rocket::post("/approve", data = "<approval>")]
pub async fn approve(task: &State<IngestTask>, conn: BlarserDbConn, approval: Form<Approval>) -> Result<Redirect, ServerError> {
    let approval_id = approval.approval_id;
    conn.run(move |c|
        set_approval(c, approval.approval_id, &approval.message, approval.approved)
    ).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    task.notify_callback(approval_id);

    Ok(Redirect::to(uri!(approvals)))
}