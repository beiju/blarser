use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use log::info;
use serde::Serialize;
use rocket::form::{Form, FromForm};
use rocket::response::{Redirect};
use rocket::serde::json::{json, Value};
use rocket::{State, uri};
use rocket::http::RawStr;
use uuid::Uuid;
use anyhow::anyhow;

use blarser::db::{BlarserDbConn, get_pending_approvals, get_latest_ingest, get_logs_for, IngestApproval, set_approval, IngestLogAndApproval};
use blarser::ingest::IngestTask;
use blarser::StateInterface;
use blarser::state::{get_recently_updated_entities, get_entity_debug};
use blarser::sim;

#[derive(rocket::Responder)]
pub enum ServerError {
    #[response(status = 500)]
    InternalError(String)
}

#[rocket::get("/")]
pub async fn index(conn: BlarserDbConn) -> Result<Template, ServerError> {
    let (ingest, logs) = conn.run(|c| {
        let ingest = get_latest_ingest(c)?;
        let logs = match &ingest {
            Some(ingest) => get_logs_for(ingest, c)?,
            None => vec![]
        };

        Ok((ingest, logs))
    }).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

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


#[rocket::get("/approvals")]
pub async fn approvals(conn: BlarserDbConn) -> Result<Template, ServerError> {
    let approvals = conn.run(|c| {
        get_pending_approvals(c)
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
pub async fn approve(_task: &State<IngestTask>, conn: BlarserDbConn, approval: Form<Approval>) -> Result<Redirect, ServerError> {
    let redirect_to = if approval.from_route == "index" {
        Ok(uri!(index))
    } else if approval.from_route == "approvals" {
        Ok(uri!(approvals))
    } else {
        Err(ServerError::InternalError(format!("Unexpected value in from_route: {}", approval.from_route)))
    }?;

    conn.run(move |c|
        set_approval(c, approval.approval_id, &approval.message, approval.approved)
    ).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    // task.notify_callback(approval_id);

    Ok(Redirect::to(redirect_to))
}

#[rocket::get("/debug")]
pub async fn debug(conn: BlarserDbConn, ingest: &State<IngestTask>) -> Result<Template, ServerError> {
    let ingest_id = ingest.latest_ingest()
        .ok_or(ServerError::InternalError(format!("There is no ingest yet")))?;

    #[derive(Serialize)]
    struct DebugEntityParams {
        name: String,
        id: Uuid,
    }

    #[derive(Serialize)]
    struct DebugTemplateParams {
        pub entities: Vec<DebugEntityParams>,
    }

    let entities = conn.run(move |c| {
        get_recently_updated_entities(c, ingest_id, 500)
    }).await
        .map_err(|e| ServerError::InternalError(anyhow!(e).context("In debug route").to_string()))?
        .into_iter()
        .map(|(entity_type, entity_id, entity_json)| DebugEntityParams {
            name: sim::entity_description(&entity_type, entity_json),
            id: entity_id
        })
        .collect();

    Ok(Template::render("debug", DebugTemplateParams { entities }))
}

#[rocket::get("/debug/<entity_id>")]
pub async fn entity_debug_json(conn: BlarserDbConn, ingest: &State<IngestTask>, entity_id: Uuid) -> Result<Value, ServerError> {
    let ingest_id = ingest.latest_ingest()
        .ok_or(ServerError::InternalError(format!("There is no ingest yet")))?;

    let (versions, parents) = conn.run(move |c| {
        get_entity_debug(c, ingest_id, entity_id)
    }).await
        .map_err(|e| ServerError::InternalError(anyhow!(e).context("In entity debug json route").to_string()))?;

    let result: Vec<_> = versions.into_iter().zip(parents)
        .map(|(version, version_parents)| {
            let parents: Vec<_> = version_parents.into_iter()
                .map(|parent| parent.parent.to_string())
                .collect();

            json!({
                "id": version.id.to_string(),
                "parentIds": parents,
            })
        })
        .collect();

    Ok(json!(result))
}