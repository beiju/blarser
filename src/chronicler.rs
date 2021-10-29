use futures::stream;
use reqwest::Client;
use serde::Deserialize;
use serde_json::value;

type ChroniclerItems = Vec<value::Value>;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChroniclerResponse {
    next_page: String,
    items: ChroniclerItems,
}

pub fn chron_versions<'a>(
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

            let resp = req.query(&cont_token)
                .send().await.unwrap()
                .json::<ChroniclerResponse>().await.unwrap();

            Some((resp.items, State::Next(resp.next_page.clone())))
        }
    }))
}