use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChroniclerItem {
    pub entity_id: Uuid,
    pub hash: String,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub data: value::Value,
}

pub type ChroniclerItems = Vec<ChroniclerItem>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChroniclerResponse {
    pub next_page: Option<String>,
    pub items: ChroniclerItems,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChroniclerV1Game {
    pub game_id: Uuid,
    pub hash: String,
    pub timestamp: DateTime<Utc>,
    pub data: value::Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChroniclerV1Response {
    pub next_page: Option<String>,
    pub data: Vec<ChroniclerV1Game>,
}
