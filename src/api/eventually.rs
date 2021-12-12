use std::collections::HashSet;
use std::sync::mpsc;
use std::thread;
use log::{info, warn};

pub use crate::api::eventually_schema::{EventuallyEvent, EventuallyResponse};

pub fn events(start: &'static str) -> impl Iterator<Item=EventuallyEvent> {
    let (sender, receiver) = mpsc::sync_channel(2);
    thread::spawn(move || events_thread(sender, start));
    receiver.into_iter()
        .flatten()
        .scan(HashSet::new(), |seen_ids, event| {
            // If this event was already seen as a sibling of a processed event, skip it
            if seen_ids.remove(&event.id) {
                info!("Discarding duplicate event {} from {}", event.description, event.created);
                // Double-option because the outer layer is used by `scan` to terminate the iterator
                return Some(None)
            }

            // seen_ids shouldn't grow very large, since every uuid that's put into it should come
            // out within a few seconds
            if seen_ids.len() > 50 {
                warn!("seen_ids is larger than expected ({} ids)", seen_ids.len());
            }

            for sibling in &event.metadata.siblings {
                if sibling.id != event.id {
                    seen_ids.insert(sibling.id);
                }
            }

            info!("Yielding event {} from {}", event.description, event.created);
            // Double-option because the outer layer is used by `scan` to terminate the iterator
            Some(Some(event))
        })
        .flatten()
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
                ("expand_siblings", "true"),
                ("sortby", "{created}"),
                ("sortorder", "asc"),
                ("after", start)
            ]);

        let request = request.build().unwrap();

        let cache_key = request.url().to_string();

        let response = match cache.get(&cache_key).unwrap() {
            Some(text) => bincode::deserialize(&text).unwrap(),
            None => {
                info!("Fetching page of feed events from network");

                let text = client
                    .execute(request).expect("Eventually API call failed")
                    .text().unwrap();

                cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

                text
            }
        };

        let response: EventuallyResponse = serde_json::from_str(&response).unwrap();


        let len = response.len();


        match sender.send(response.0) {
            Ok(_) => { },
            Err(err) => {
                warn!("Exiting eventually thread due to {:?}", err);
                return;
            }
        }
        if len < PAGE_SIZE {
            break;
        }

        page = page + 1;
    }
}