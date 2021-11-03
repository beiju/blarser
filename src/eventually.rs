use std::sync::mpsc;
use std::thread;

use crate::eventually_schema::{EventuallyResponse, EventuallyEvent};

pub fn events(start: &'static str) -> impl Iterator<Item=EventuallyEvent> {
    let (sender, receiver) = mpsc::sync_channel(2);
    thread::spawn(move || events_thread(sender, start) );
    receiver.into_iter().flatten()
}

fn events_thread(sender: mpsc::SyncSender<Vec<EventuallyEvent>>, start: &str) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page = 0;
    const PAGE_SIZE: usize = 100;

    loop {
        println!("Fetching page of feed events");
        let response: EventuallyResponse = client.get("https://api.sibr.dev/eventually/v2/events")
            .query(&[
                ("limit", PAGE_SIZE),
                ("offset", page * PAGE_SIZE),
            ]).query(&[
            ("sortby", "{created}"),
            ("sortorder", "asc"),
            ("after", start)
        ])
            .send().expect("Eventually API call failed")
            .json().expect("Eventually JSON decode failed");

        let len = response.len();
        sender.send(response.0).unwrap();

        if len < PAGE_SIZE {
            break;
        }

        page = page + 1;
    }
}
