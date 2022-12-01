use rocket::{State, error};
use uuid::Uuid;
use serde_json::{Value, json};
use text_diff::Difference;
use rocket_dyn_templates::Template;
use blarser::{entity, entity_dispatch};
use itertools::Itertools;
use serde::Serialize;
use anyhow::anyhow;
use im::HashMap;

use blarser::db::BlarserDbConn;
use blarser::ingest::IngestTaskHolder;
use blarser::state::{get_entity_debug, VersionLink, Version, StateInterface, EntityDescription};
use crate::routes::ApiError;

#[rocket::get("/debug")]
pub async fn debug(conn: BlarserDbConn, ingest_holder: &State<IngestTaskHolder>) -> Result<Template, ApiError> {
    let ingest_id = ingest_holder.latest_ingest_id()
        .ok_or_else(|| ApiError::InternalError("There is no ingest yet".to_string()))?;

    #[derive(Serialize)]
    struct DebugTemplateParams {
        pub entities: Vec<EntityDescription>,
    }

    let entities = conn.run(move |c| {
        let state = StateInterface::new(c, ingest_id);
        state.get_recently_updated_entity_descriptions(500)
    }).await
        .map_err(|e| {
            error!("Diesel error: {}", e);
            ApiError::InternalError(anyhow!(e).context("In debug route").to_string())
        })?;

    Ok(Template::render("debug", DebugTemplateParams { entities }))
}

#[rocket::get("/debug/<entity_type>/<entity_id>")]
pub async fn entity_debug_json(conn: BlarserDbConn, ingest: &State<IngestTaskHolder>, entity_type: String, entity_id: kkkkkkkkkkkkkkkkkkkkkkkkkUuid) -> Result<Value, ApiError> {
    let ingest_id = ingest.latest_ingest_id()
        .ok_or_else(|| ApiError::InternalError("There is no ingest yet".to_string()))?;

    let versions_info = conn.run(move |c| {
        let state = StateInterface::new(c, ingest_id);
        state.get_entity_debug(&entity_type, entity_id)
    }).await
        .map_err(|e| ApiError::InternalError(anyhow!(e).context("In entity debug json route").to_string()))?;

    Ok(json!(versions_info))
}

// fn span_wrap(string: &str, class_name: &str) -> String {
//     string.lines().map(|val| {
//         format!("<span class=\"{}\">{}\n</span>", class_name, val)
//     }).join("")
// }
//
// fn build_json(prev_entity_str: &mut String, version: &Version, event: DbEvent, version_parents: Vec<VersionLink>) -> Result<serde_json::Value, anyhow::Error> {
//     let parents: Vec<_> = version_parents.into_iter()
//         .map(|parent| parent.parent.to_string())
//         .collect();
//
//     let event = event.parse()
//         .map_err(|e| anyhow!(e))?;
//
//     let entity_str = serde_json::to_string_pretty(&version.data)
//         .map_err(|e| anyhow!(e))?;
//
//     let (_, diff) = text_diff::diff(prev_entity_str, &*entity_str, "\n");
//     let diff_str = diff.into_iter().map(|d| {
//         match d {
//             Difference::Same(val) => { span_wrap(&val, "diff-same") }
//             Difference::Add(val) => { span_wrap(&val, "diff-add") }
//             Difference::Rem(val) => { span_wrap(&val, "diff-rem") }
//         }
//     }).join("");
//     *prev_entity_str = entity_str;
//
//     Ok(json!({
//         "id": version.id.to_string(),
//         "event": event.to_string(),
//         "type": event.type_str(),
//         "diff": diff_str,
//         "parentIds": parents,
//         "terminated": version.terminated,
//         "observations": version.observations,
//     }))
// }
