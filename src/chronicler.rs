use chrono::{DateTime, Utc};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;
use reqwest::blocking::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::value;
use crate::chronicler_schema::{ChroniclerItem, ChroniclerItems, ChroniclerResponse};

pub fn versions(entity_type: &'static str, start: &'static str) -> Receiver<ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || versions_thread(sender, entity_type, start));
    receiver
}

fn versions_thread(sender: SyncSender<ChroniclerItem>, entity_type: &str, start: &str) -> () {
    let client = Client::new();

    let mut page: Option<String> = None;

    loop {
        let response = match page {
            Some(page) => chron_get(&client, &[
                ("type", &entity_type),
                ("after", &start),
                ("page", &page.as_str()),
            ]),
            None => chron_get(&client, &[
                ("type", &entity_type),
                ("after", &start)
            ])
        };

        page = Some(response.next_page);

        for item in response.items {
            sender.send(item).unwrap();
        }
    }
}

fn chron_get<T: Serialize + ?Sized>(client: &Client, params: &T) -> ChroniclerResponse {
    client.get("https://api.sibr.dev/chronicler/v2/versions")
        .query(params)
        .send().expect("Chronicler API call failed")
        .json::<ChroniclerResponse>().expect("Chronicler JSON decode failed")
}