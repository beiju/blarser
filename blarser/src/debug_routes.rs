use std::cmp::Reverse;
use std::ops::Deref;
use itertools::Itertools;
use rocket::{get, Request, response, Route, State};
use rocket::http::Status;
use rocket::response::Responder;
use rocket::serde::json::Json;
use serde_json::{json, Value};
use thiserror::Error;
use uuid::Uuid;
use blarser::ingest::{GraphDebugHistorySync, GraphDebugHistory, IngestTaskHolder};
use blarser::state::EntityType;

#[derive(Debug, Error)]
pub enum DebugApiError {
    #[error("The lock was poisoned!")]
    LockPoisoned,

    #[error("No active ingest!")]
    NoActiveIngest,

    #[error("Invalid entity type {0}")]
    InvalidEntityType(String),

    #[error("Invalid entity {ty} {id}")]
    InvalidEntity {
        ty: EntityType,
        id: Uuid,
    },

    #[error("Invalid version {index} for entity {ty} {id}")]
    InvalidEntityVersion {
        ty: EntityType,
        id: Uuid,
        index: usize,
    },
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
        .sorted_by_key(|(_, item)| Reverse(item.versions.last().unwrap().time))
        .take(500)
        .map(|((ty, id), item)| json!({
            "name": item.entity_human_name,
            "type": ty,
            "id": id
        }))
        .collect();

    Value::Array(items)
}

#[get("/entity/<entity_type>/<id>")]
pub async fn entity(task: &State<IngestTaskHolder>, entity_type: String, id: Uuid) -> Result<Json<serde_json::Value>, DebugApiError> {
    let history = get_history(task)?;
    let history = history.lock().await;

    let entity_type = match entity_type.as_str() {
        "Sim" => EntityType::Sim,
        "Player" => EntityType::Player,
        "Team" => EntityType::Team,
        "Game" => EntityType::Game,
        "Standings" => EntityType::Standings,
        "Season" => EntityType::Season,
        _ => return Err(DebugApiError::InvalidEntityType(entity_type))
    };

    Ok(Json(get_history_entity(history.deref(), entity_type, id)?))
}

fn get_history_entity(history: &GraphDebugHistory, entity_type: EntityType, id: Uuid) -> Result<Value, DebugApiError> {
    let items = history.get(&(entity_type, id))
        .ok_or_else(|| DebugApiError::InvalidEntity { ty: entity_type, id })?
        .versions.iter()
        .enumerate()
        .rev()
        .take(500)
        .map(|(i, v)| json!({
            "name": v.event_human_name,
            "index": i,
        }))
        .collect();

    Ok(Value::Array(items))
}

#[get("/version/<entity_type>/<id>/<index>")]
pub async fn version(task: &State<IngestTaskHolder>, entity_type: String, id: Uuid, index: usize) -> Result<Json<serde_json::Value>, DebugApiError> {
    let history = get_history(task)?;
    let history = history.lock().await;

    let entity_type = match entity_type.as_str() {
        "Sim" => EntityType::Sim,
        "Player" => EntityType::Player,
        "Team" => EntityType::Team,
        "Game" => EntityType::Game,
        "Standings" => EntityType::Standings,
        "Season" => EntityType::Season,
        _ => return Err(DebugApiError::InvalidEntityType(entity_type))
    };

    Ok(Json(get_history_version(history.deref(), entity_type, id, index)?.clone()))
}

fn get_history_version(history: &GraphDebugHistory, entity_type: EntityType, id: Uuid, index: usize) -> Result<Value, DebugApiError> {
    let version = history.get(&(entity_type, id))
        .ok_or_else(|| DebugApiError::InvalidEntity { ty: entity_type, id })?
        .versions.get(index)
        .ok_or_else(|| DebugApiError::InvalidEntityVersion { ty: entity_type, id, index })?;

    Ok(serde_json::to_value(version).unwrap())
}

fn get_history(task: &State<IngestTaskHolder>) -> Result<GraphDebugHistorySync, DebugApiError> {
    let ingest = task.latest_ingest.lock().map_err(|_| DebugApiError::LockPoisoned)?;
    let ingest = ingest.as_ref().ok_or_else(|| DebugApiError::NoActiveIngest)?;
    Ok(ingest.debug_history.clone())
}

pub fn routes() -> Vec<Route> {
    rocket::routes![entities, entity, version]
}