use std::io::{Cursor, Read, Write};
use std::ops::Deref;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rocket::{State, FromForm, form::{self, FromFormField}};
use serde::{Serialize, Serializer};
use serde::ser::Error;
use serde_json::{Value, json, Map};
use uuid::Uuid;
use diesel::prelude::*;
use diesel::Queryable;

use blarser::db::BlarserDbConn;
use blarser::ingest::IngestTaskHolder;
use blarser::sim::entity_to_raw_approximation;
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

impl UuidList {
    pub fn into_inner(self) -> Vec<Uuid> { self.0 }
}

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

pub struct PageToken {
    id: Uuid,
    time: DateTime<Utc>,
}

fn blaseball_epoch() -> DateTime<Utc> {
    Utc.ymd(2020, 7, 1).and_hms(0, 0, 0)
}

#[rocket::async_trait]
impl<'r> FromFormField<'r> for PageToken {
    fn from_value(field: form::ValueField<'r>) -> form::Result<'r, Self> {
        let bytes = base64::decode(field.value.replace('-', "+").replace('_', "/"))
            .map_err(|e| form::Error::validation(e.to_string()))?;

        if bytes.len() != 24 {
            return Err(form::Error::validation("must be 24 bytes long".to_string()).into());
        }

        let mut bytes = Cursor::new(bytes);

        let mut uuid_bytes: [u8; 16] = Default::default();
        bytes.read_exact(&mut uuid_bytes)
            .map_err(|e| form::Error::validation(e.to_string()))?;
        let uuid = Uuid::from_bytes(uuid_bytes);

        let time_offset = bytes.read_i64::<NativeEndian>()
            .map_err(|e| form::Error::validation(e.to_string()))?;
        let time = blaseball_epoch() + Duration::nanoseconds(time_offset * 100);

        Ok(Self { id: uuid, time })
    }
}

impl Serialize for PageToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut bytes = Cursor::new(Vec::<u8>::new());
        bytes.write(self.id.as_bytes())
            .map_err(|e| S::Error::custom(e.to_string()))?;

        // TODO Implement a num_ticks in the style of num_nanoseconds and that won't require checked
        //  multiplication
        let time_offset = (self.time - blaseball_epoch()).num_nanoseconds().map(|n| n / 100)
            .expect("time_offset overflowed");
        bytes.write_i64::<NativeEndian>(time_offset)
            .map_err(|e| S::Error::custom(e.to_string()))?;

        // TODO Use a Config object to get the proper character set instead of hacky string replace
        let str = base64::encode(bytes.into_inner()).replace('+', "-").replace('/', "_");
        serializer.serialize_str(&str)
    }
}

#[derive(FromForm)]
pub struct EntitiesParams {
    r#type: String,
    at: Option<ParseableDateTime>,
    count: Option<i64>,
    id: Option<UuidList>,
    page: Option<PageToken>,
    all: Option<bool>,
}

#[derive(Serialize, Queryable)]
#[serde(rename = "camelCase")]
pub struct EntityVersions {
    pub entity_id: Uuid,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub data: Vec<Value>,
}

#[derive(Serialize)]
#[serde(rename = "camelCase")]
pub struct EntityVersion {
    pub entity_id: Uuid,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub data: Value,
}

impl EntityVersion {
    fn from_versions(entity_type: &str, versions: EntityVersions) -> Self {
        Self {
            entity_id: versions.entity_id,
            valid_from: versions.valid_from,
            valid_to: versions.valid_to,
            data: versions.data.into_iter().next()
                .map(|value| {
                    entity_to_raw_approximation(entity_type, value)
                })
                .unwrap_or_else(|| Value::Object(Map::new())),
        }
    }
}

#[rocket::get("/entities?<params..>")]
pub async fn entities(conn: BlarserDbConn, ingest: &State<IngestTaskHolder>, params: Result<EntitiesParams, form::Errors<'_>>) -> Result<Value, ApiError> {
    let params = params.map_err(|e| ApiError::ParseError(e.to_string()))?;
    let ingest_id = ingest.latest_ingest_id()
        .ok_or_else(|| ApiError::InternalError("No ingest yet".to_string()))?;

    let wants_all = params.all.unwrap_or(false);
    let entity_type = params.r#type.clone();
    let results = conn.run(move |c| {
        use blarser::schema::versions_with_range::dsl as versions;
        use diesel::dsl::sql;
        use diesel::sql_types::{Array, Jsonb};

        // Need to repeat this part because distinct_on fails to compile on a boxed query
        let query = versions::versions_with_range
            // Group results for each entity (end_time should never create a separate group)
            .group_by((versions::entity_id, versions::event_time, versions::end_time))
            .select((versions::entity_id, versions::event_time, versions::end_time, sql::<Array<Jsonb>>("array_agg(data) AS data")))
            // Is from the right ingest
            .filter(versions::ingest_id.eq(ingest_id))
            // Has the right entity type
            .filter(versions::entity_type.eq(params.r#type))
            // Has not been terminated
            .filter(versions::terminated.is_null())
            // Order by id, necessary for page_token
            .order(versions::entity_id)
            .limit(params.count.unwrap_or(100))
            .into_boxed();

        let query = if let Some(time) = params.at {
            query
                // Was created before the requested time
                // This needs to be lt, rather than le, to work correctly in FeedStateInterface::read_entity
                .filter(versions::event_time.lt(*time))
                // Has no children, or at least one child is after the requested time
                // This needs to be ge, rather than gt, to work correctly in FeedStateInterface::read_entity
                .filter(versions::end_time.is_null().or(versions::end_time.ge(*time)))
        } else {
            // No time specified = latest version only
            query.filter(versions::end_time.is_null())
        };

        let query = if let Some(ids) = params.id {
            query.filter(versions::entity_id.eq_any(ids.into_inner()))
        } else {
            query
        };

        let query = if let Some(page) = params.page {
            query.filter(versions::entity_id.gt(page.id))
        } else {
            query
        };

        query.load::<EntityVersions>(c)
    }).await
        .map_err(|e| ApiError::InternalError(e.to_string()))?;

    let next_page = results.last()
        .map(|v| PageToken { id: v.entity_id, time: v.valid_from });

    if wants_all {
        Ok(json!({
            "nextPage": next_page,
            "items": results
        }))
    } else {
        let results: Vec<_> = results.into_iter()
            .map(|v| EntityVersion::from_versions(&entity_type, v))
            .collect();
        Ok(json!({
            "nextPage": next_page,
            "items": results
        }))
    }
}