use itertools::Itertools;
use rocket_dyn_templates::Template;
use diesel::result::Error as DieselError;
use serde::Serialize;
use rocket::form::{Form, FromForm};
use rocket::response::{Redirect};
use rocket::serde::json::{json, Value};
use rocket::{error, State, uri};
use uuid::Uuid;
use anyhow::anyhow;
use text_diff::Difference;

use blarser::db::{IngestApproval, IngestLogAndApproval, BlarserDbConn, get_pending_approvals, get_latest_ingest, get_logs_for, set_approval};
use blarser::ingest::IngestTask;
use blarser::state::{Version, Event, Parent, get_recently_updated_entities, get_entity_debug};
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
pub async fn approve(task: &State<IngestTask>, conn: BlarserDbConn, approval: Form<Approval>) -> Result<Redirect, ServerError> {
    let redirect_to = if approval.from_route == "index" {
        Ok(uri!(index))
    } else if approval.from_route == "approvals" {
        Ok(uri!(approvals))
    } else {
        Err(ServerError::InternalError(format!("Unexpected value in from_route: {}", approval.from_route)))
    }?;

    let approval_id = approval.approval_id;
    let approved = approval.approved;
    conn.run(move |c|
        set_approval(c, approval.approval_id, &approval.message, approval.approved)
    ).await
        .map_err(|err: DieselError| ServerError::InternalError(err.to_string()))?;

    task.notify_approval(approval_id, approved);

    Ok(Redirect::to(redirect_to))
}

#[rocket::get("/debug")]
pub async fn debug(conn: BlarserDbConn, ingest: &State<IngestTask>) -> Result<Template, ServerError> {
    let ingest_id = ingest.latest_ingest()
        .ok_or(ServerError::InternalError(format!("There is no ingest yet")))?;

    #[derive(Serialize)]
    struct DebugEntityParams {
        name: String,
        r#type: String,
        id: Uuid,
    }

    #[derive(Serialize)]
    struct DebugTemplateParams {
        pub entities: Vec<DebugEntityParams>,
    }

    let entities = conn.run(move |c| {
        get_recently_updated_entities(c, ingest_id, 500)
    }).await
        .map_err(|e| {
            error!("Diesel error: {}", e);
            ServerError::InternalError(anyhow!(e).context("In debug route").to_string())
        })?
        .into_iter()
        .map(|(entity_type, entity_id, entity_json)| DebugEntityParams {
            name: sim::entity_description(&entity_type, entity_json),
            r#type: entity_type,
            id: entity_id,
        })
        .collect();

    Ok(Template::render("debug", DebugTemplateParams { entities }))
}

#[rocket::get("/debug/<entity_type>/<entity_id>")]
pub async fn entity_debug_json(conn: BlarserDbConn, ingest: &State<IngestTask>, entity_type: String, entity_id: Uuid) -> Result<Value, ServerError> {
    let ingest_id = ingest.latest_ingest()
        .ok_or(ServerError::InternalError(format!("There is no ingest yet")))?;

    let versions_info = conn.run(move |c| {
        get_entity_debug(c, ingest_id, &entity_type, entity_id)
    }).await
        .map_err(|e| ServerError::InternalError(anyhow!(e).context("In entity debug json route").to_string()))?;

    let result: Vec<_> = versions_info.into_iter()
        .scan(String::from(""), |prev_entity_str, (version, event, version_parents)| {
            Some(build_json(prev_entity_str, &version, event, version_parents))
        })
        .try_collect()
        .map_err(|e| ServerError::InternalError(e.context("In entity debug json route").to_string()))?;

    Ok(json!(result))
}

fn span_wrap(string: &str, class_name: &str) -> String {
    string.lines().map(|val| {
        format!("<span class=\"{}\">{}\n</span>", class_name, val)
    }).join("")
}

fn build_json(prev_entity_str: &mut String, version: &Version, event: Event, version_parents: Vec<Parent>) -> Result<serde_json::Value, anyhow::Error> {
    let parents: Vec<_> = version_parents.into_iter()
        .map(|parent| parent.parent.to_string())
        .collect();

    let event = event.parse()
        .map_err(|e| anyhow!(e))?;

    let entity_str = serde_json::to_string_pretty(&version.data)
        .map_err(|e| anyhow!(e))?;

    let (_, diff) = text_diff::diff(prev_entity_str, &*entity_str, "\n");
    let diff_str = diff.into_iter().map(|d| {
        match d {
            Difference::Same(val) => { span_wrap(&val, "diff-same") }
            Difference::Add(val) => { span_wrap(&val, "diff-add") }
            Difference::Rem(val) => { span_wrap(&val, "diff-rem") }
        }
    }).join("");
    *prev_entity_str = entity_str;

    Ok(json!({
        "id": version.id.to_string(),
        "event": event.to_string(),
        "type": event.type_str(),
        "diff": diff_str,
        "parentIds": parents,
        "terminated": version.terminated,
        "observations": version.observations,
    }))
}