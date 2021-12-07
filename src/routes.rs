use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use serde::Serialize;
use rocket::form::{Form, FromForm};
use rocket::response::Redirect;
use rocket::{State, uri};


use blarser::db::{BlarserDbConn, get_pending_approvals, get_latest_ingest, get_logs_for, IngestApproval, set_approval, IngestLogAndApproval};
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
        logs: Vec<IngestLogAndApproval>,
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
    let approvals = conn.run(|c| {
        get_pending_approvals(&c)
    }).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    #[derive(Serialize)]
    struct ApprovalTemplateParams {
        approvals: Vec<IngestApproval>,
    }

    Ok(Template::render("approvals", ApprovalTemplateParams {
        approvals,
    }))
}

#[derive(FromForm)]
pub struct Approval {
    approval_id: i32,
    message: String,
    approved: bool,
    from_route: String, // Todo try to figure out how to get this to be an enum
}

#[rocket::post("/approve", data = "<approval>")]
pub async fn approve(task: &State<IngestTask>, conn: BlarserDbConn, approval: Form<Approval>) -> Result<Redirect, ServerError> {
    let redirect_to = if approval.from_route == "index" {
        Ok(uri!(index))
    } else if approval.from_route == "approvals" {
        Ok(uri!(approvals))
    } else {
        Err(ServerError::InternalError(format!("Unexpected value in from_route: {}", approval.from_route)))
    }?;

    let approval_id = approval.approval_id;
    conn.run(move |c|
        set_approval(c, approval.approval_id, &approval.message, approval.approved)
    ).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    task.notify_callback(approval_id);

    Ok(Redirect::to(redirect_to))
}