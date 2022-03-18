use std::ops::Deref;
use chrono::{DateTime, Utc};
use rocket::{FromForm, form::{self, FromFormField}};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::routes::ApiError;

pub struct ParseableDateTime(DateTime<Utc>);

#[rocket::async_trait]
impl<'r> FromFormField<'r> for ParseableDateTime {
    fn from_value(field: form::ValueField<'r>) -> form::Result<'r, Self> {
        DateTime::parse_from_rfc3339(field.value)
            .map(|d| Self(d.with_timezone(&Utc)))
            .map_err(|e| form::Error::validation(e.to_string()).into())
    }
}

impl Deref for ParseableDateTime {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct UuidList(Vec<Uuid>);

#[rocket::async_trait]
impl<'r> FromFormField<'r> for UuidList {
    fn from_value(field: form::ValueField<'r>) -> form::Result<'r, Self> {
        let uuids = field.value.split(',')
            .map(|id_str| Uuid::parse_str(id_str)
                .map_err(|e| form::Error::validation(e.to_string()).into()))
            .collect::<form::Result<_>>()?;
        Ok(Self(uuids))
    }
}

impl Deref for UuidList {
    type Target = Vec<Uuid>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(FromForm)]
pub struct EntitiesParams {
    r#type: String,
    at: Option<ParseableDateTime>,
    count: Option<u32>,
    id: Option<UuidList>,
    page: Option<String>,
}


#[rocket::get("/entities?<params..>")]
pub async fn entities(params: Result<EntitiesParams, form::Errors<'_>>) -> Result<Value, ApiError> {
    let params = params.map_err(|e| ApiError::ParseError(e.to_string()))?;

    Ok(json!({
        "type": params.r#type,
        "at": params.at.as_deref(),
        "count": params.count,
        "id": params.id.as_deref(),
        "page": params.page
    }))
}