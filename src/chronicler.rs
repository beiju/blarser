use std::sync::mpsc;
use std::thread;
use serde::Serialize;

use crate::chronicler_schema::{ChroniclerItem, ChroniclerResponse};

pub fn versions(entity_type: &'static str, start: &'static str) -> mpsc::Receiver<ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || chron_thread(sender, "https://api.sibr.dev/chronicler/v2/versions", &[
        ("type", &entity_type),
        ("after", &start)
    ]));
    receiver
}

pub fn entities(entity_type: &'static str, start: &'static str) -> mpsc::Receiver<ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || chron_thread(sender, "https://api.sibr.dev/chronicler/v2/entities", &[
        ("type", &entity_type),
        ("at", &start)
    ]));
    receiver
}

fn chron_thread<T: Serialize + ?Sized>(
    sender: mpsc::SyncSender<ChroniclerItem>, url: &'static str, params: &T,
) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page: Option<String> = None;

    loop {
        let request = client.get(url).query(params);

        let request = match page {
            Some(page) => request.query(&[("page", page)]),
            None => request
        };

        let response = request
            .send().expect("Chronicler API call failed");

        let text = response.text().unwrap();
        println!("{}", text.chars().into_iter().take(200).collect::<String>());

        let response: ChroniclerResponse = serde_json::from_str(&text).expect("Chronicler JSON decode failed");

        for item in response.items {
            sender.send(item).unwrap();
        }

        page = match response.next_page {
            Some(p) => Some(p),
            None => return
        }
    }
}