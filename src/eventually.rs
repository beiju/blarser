use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;

use crate::eventually_schema::{EventuallyResponse, EventuallyEvent};

pub fn events(start: &'static str) -> Receiver<EventuallyEvent> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || events_thread(sender, start) );
    receiver
}

fn events_thread(sender: mpsc::SyncSender<EventuallyEvent>, start: &str) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page = 0;
    const PAGE_SIZE: usize = 100;

    loop {
        let response = client.get("https://api.sibr.dev/eventually/v2/events")
            .query(&[
                ("limit", PAGE_SIZE),
                ("offset", page * PAGE_SIZE),
            ]).query(&[
            ("sortby", "{created}"),
            ("sortorder", "asc"),
            ("after", start)
        ])
            .send().expect("Eventually API call failed")
            .json::<EventuallyResponse>().expect("Eventually JSON decode failed");

        let len = response.len();

        for event in response.into_iter() {
            sender.send(event).unwrap();
        }

        if len < PAGE_SIZE {
            break;
        }

        page = page + 1;
    }
}
