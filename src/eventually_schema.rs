use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct EventuallyResponse(Vec<EventuallyEvent>);

impl EventuallyResponse {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl IntoIterator for EventuallyResponse {
    type Item = EventuallyEvent;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EventuallyEvent {
    pub created: DateTime<Utc>,
    pub r#type: i32,
    pub category: i32,
    pub description: String,
}