use std::cmp::Reverse;
use std::ops::Deref;
use itertools::Itertools;
use rocket::{get, Request, response, Route, State};
use rocket::http::Status;
use rocket::response::Responder;
use rocket::serde::json::Json;
use serde_json::{json, Value};
use thiserror::Error;
use blarser::ingest::{GraphDebugHistorySync, GraphDebugHistory, IngestTaskHolder};

#[derive(Debug, Error)]
enum DebugApiError {
    #[error("The lock was poisoned!")]
    LockPoisoned,

    #[error("No active ingest!")]
    NoActiveIngest,
}

impl<'r, 'o: 'r> Responder<'r, 'o> for DebugApiError {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'o> {
        // log `self` to your favored error tracker, e.g.
        // sentry::capture_error(&self);

        match self {
            // in our simplistic example, we're happy to respond with the default 500 responder in all cases
            _ => Status::InternalServerError.respond_to(req)
        }
    }
}


#[get("/entities")]
pub async fn entities(task: &State<IngestTaskHolder>) -> Result<Json<serde_json::Value>, DebugApiError> {
    let history = get_history(task)?;
    let history = history.lock().await;

    Ok(Json(get_history_entities(history.deref())))
}

fn get_history_entities(history: &GraphDebugHistory) -> Value {
    let items = history.iter()
        .sorted_by_key(|(_, item)| Reverse(item.time))
        .take(500)
        .map(|((ty, id), item)| json!({
            "name": item.entity_human_name,
            "type": ty,
            "id": id
        }))
        .collect();

    Value::Array(items)
}

fn get_history(task: &State<IngestTaskHolder>) -> Result<GraphDebugHistorySync, DebugApiError> {
    let ingest = task.latest_ingest.lock().map_err(|_| DebugApiError::LockPoisoned)?;
    let ingest = ingest.as_ref().ok_or_else(|| DebugApiError::NoActiveIngest)?;
    Ok(ingest.debug_history.clone())
}

pub fn routes() -> Vec<Route> {
    rocket::routes![entities]
}