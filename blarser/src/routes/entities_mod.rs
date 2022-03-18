use std::io::{Cursor, Read, Write};
use std::ops::Deref;
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rocket::{FromForm, form::{self, FromFormField}};
use serde::{Serialize, Serializer};
use serde::ser::Error;
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
    count: Option<u32>,
    id: Option<UuidList>,
    page: Option<PageToken>,
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