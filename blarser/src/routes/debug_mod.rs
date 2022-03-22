// use rocket::{State, error};
// use uuid::Uuid;
// use serde_json::{Value, json};
// use text_diff::Difference;
// use rocket_dyn_templates::Template;
// use blarser::{entity, entity_dispatch};
// use itertools::Itertools;
// use serde::Serialize;
// use anyhow::anyhow;
//
// use blarser::db::BlarserDbConn;
// use blarser::ingest::IngestTaskHolder;
// use blarser::state::{get_entity_debug, get_recently_updated_entities, VersionLink, Version};
// use crate::routes::ApiError;
//
// #[rocket::get("/debug")]
// pub async fn debug(conn: BlarserDbConn, ingest_holder: &State<IngestTaskHolder>) -> Result<Template, ApiError> {
//     let ingest_id = ingest_holder.latest_ingest_id()
//         .ok_or_else(|| ApiError::InternalError("There is no ingest yet".to_string()))?;
//
//     #[derive(Serialize)]
//     struct DebugEntityParams {
//         name: String,
//         r#type: String,
//         id: Uuid,
//     }
//
//     #[derive(Serialize)]
//     struct DebugTemplateParams {
//         pub entities: Vec<DebugEntityParams>,
//     }
//
//     let entities = conn.run(move |c| {
//         get_recently_updated_entities(c, ingest_id, 500)
//     }).await
//         .map_err(|e| {
//             error!("Diesel error: {}", e);
//             ApiError::InternalError(anyhow!(e).context("In debug route").to_string())
//         })?
//         .into_iter()
//         .map(|(entity_type, entity_id, entity_json)| DebugEntityParams {
//             name: entity::entity_description(&entity_type, entity_json),
//             r#type: entity_type,
//             id: entity_id,
//         })
//         .collect();
//
//     Ok(Template::render("debug", DebugTemplateParams { entities }))
// }
//
// #[rocket::get("/debug/<entity_type>/<entity_id>")]
// pub async fn entity_debug_json(conn: BlarserDbConn, ingest: &State<IngestTaskHolder>, entity_type: String, entity_id: Uuid) -> Result<Value, ApiError> {
//     let ingest_id = ingest.latest_ingest_id()
//         .ok_or_else(|| ApiError::InternalError("There is no ingest yet".to_string()))?;
//
//     let versions_info = conn.run(move |c| {
//         entity_dispatch!(entity_type => get_entity_debug(c, ingest_id, &entity_type, entity_id);
//                          other => return ApiError::InternalError(anyhow!("Incorrect entity type")));
//
//     }).await
//         .map_err(|e| ApiError::InternalError(anyhow!(e).context("In entity debug json route").to_string()))?;
//
//     let result: Vec<_> = versions_info.into_iter()
//         .scan(String::from(""), |prev_entity_str, (version, event, version_parents)| {
//             Some(build_json(prev_entity_str, &version, event, version_parents))
//         })
//         .try_collect()
//         .map_err(|e| ApiError::InternalError(e.context("In entity debug json route").to_string()))?;
//
//     Ok(json!(result))
// }
//
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
