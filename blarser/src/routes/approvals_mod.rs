use rocket::{
    form::{Form, FromForm},
    State,
    response::Redirect,
    uri
};
use diesel::result::Error as DieselError;
use rocket_dyn_templates::Template;
use serde::Serialize;

use blarser::ingest::IngestTaskHolder;
use blarser::db::{BlarserDbConn, get_pending_approvals, Approval, set_approval};
use crate::routes::{ApiError, rocket_uri_macro_index};

#[rocket::get("/approvals")]
pub async fn approvals(conn: BlarserDbConn) -> Result<Template, ApiError> {
    let approvals = conn.run(|c| {
        get_pending_approvals(c)
    }).await
        .map_err(|err: DieselError| ApiError::InternalError(err.to_string()))?;

    #[derive(Serialize)]
    struct ApprovalTemplateParams {
        approvals: Vec<Approval>,
    }

    Ok(Template::render("approvals", ApprovalTemplateParams {
        approvals,
    }))
}

#[derive(FromForm)]
pub struct ApprovalForm {
    approval_id: i32,
    message: String,
    approved: bool,
    from_route: String, // Todo try to figure out how to get this to be an enum
}

#[rocket::post("/approve", data = "<approval>")]
pub async fn approve(task: &State<IngestTaskHolder>, conn: BlarserDbConn, approval: Form<ApprovalForm>) -> Result<Redirect, ApiError> {
    let redirect_to = if approval.from_route == "index" {
        Ok(uri!(index))
    } else if approval.from_route == "approvals" {
        Ok(uri!(approvals))
    } else {
        Err(ApiError::InternalError(format!("Unexpected value in from_route: {}", approval.from_route)))
    }?;

    let approval_id = approval.approval_id;
    let approved = approval.approved;
    conn.run(move |c|
        set_approval(c, approval.approval_id, &approval.message, approval.approved)
    ).await
        .map_err(|err: DieselError| ApiError::InternalError(err.to_string()))?;

    task.notify_approval(approval_id, approved);

    Ok(Redirect::to(redirect_to))
}
