use std::sync::mpsc;
use std::thread;

use crate::eventually_schema::{EventuallyResponse, EventuallyEvent};

pub fn events(start: &'static str) -> impl Iterator<Item=EventuallyEvent> {
    let (sender, receiver) = mpsc::sync_channel(2);
    thread::spawn(move || events_thread(sender, start));
    receiver.into_iter().flatten()
}

fn events_thread(sender: mpsc::SyncSender<Vec<EventuallyEvent>>, start: &str) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page = 0;
    const PAGE_SIZE: usize = 100;
    let cache: sled::Db = sled::open("http_cache/eventually/").unwrap();

    loop {
        let request = client.get("https://api.sibr.dev/eventually/v2/events")
            .query(&[
                ("limit", PAGE_SIZE),
                ("offset", page * PAGE_SIZE),
            ])
            .query(&[
                ("sortby", "{created}"),
                ("sortorder", "asc"),
                ("after", start)
            ]);

        let request = request.build().unwrap();

        let cache_key = request.url().to_string();

        let response = match cache.get(&cache_key).unwrap() {
            Some(text) => bincode::deserialize(&text).unwrap(),
            None => {
                println!("Fetching page of feed events from network");

                let text = client
                    .execute(request).expect("Eventually API call failed")
                    .text().unwrap();

                cache.insert(&cache_key, bincode::serialize(&text).unwrap());

                text
            }
        };

        let response: EventuallyResponse = serde_json::from_str(&response).unwrap();


        let len = response.len();
        sender.send(response.0).unwrap();

        if len < PAGE_SIZE {
            break;
        }

        page = page + 1;
    }
}
