use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use serde_json::value;
use crate::chronicler_schema::{ChroniclerItem, ChroniclerItems, ChroniclerResponse};

pub fn chron_versions<'a>(
    client: &'a Client, entity_type: &'a str, after: &'a str,
) -> impl stream::Stream<Item=ChroniclerItem> + 'a
{
    chron_pages(client, entity_type, after)
        .flat_map(|items| stream::iter(items))
}

fn chron_pages<'a>(
    client: &'a Client, entity_type: &'a str, after: &'a str,
) -> impl stream::Stream<Item=ChroniclerItems> + 'a
{
    enum State {
        Start,
        Next(String),
        End,
    }

    Box::pin(stream::unfold(State::Start, move |state| {
        let req = client.get("https://api.sibr.dev/chronicler/v2/versions")
            .query(&[
                ("type", &entity_type),
                ("after", &after)
            ]);

        async move {
            let cont_token = match state {
                State::Start => vec![],
                State::Next(ct) => vec![("page", ct)],
                State::End => return None,
            };

            let req = req.query(&cont_token);

            match chron_request(req).await {
                Ok(r) => Some((r.items, State::Next(r.next_page))),
                Err(e) => {
                    dbg!("{}", e);
                    None
                }
            }
        }
    }))
}

async fn chron_request(req: RequestBuilder) -> reqwest::Result<ChroniclerResponse> {
    req
        .send().await?
        .json::<ChroniclerResponse>().await
}