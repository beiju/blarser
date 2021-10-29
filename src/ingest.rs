use std::error::Error;
use std::sync::mpsc;
use std::thread;
use crate::chronicler::chron_versions;
use futures::StreamExt;
use crate::eventually_schema::{EventuallyResponse, IngestObject};

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub async fn ingest() -> Result<(), Box<dyn Error>> {
    // let (sender, receiver) = mpsc::sync_channel(16);

    // thread::spawn(move || {
    //     get_merged_ingest_objects(sender)
    // });

    let client = reqwest::Client::new();
    let mut stream = chron_versions(&client, "player", EXPANSION_ERA_START);

    while let Some(response) = stream.next().await {
        println!("Got one: {:?}", response);
    }

    Ok(())

    // loop {
    //     match receiver.recv()? {
    //         IngestObject::EventuallyEvent(e) => println!("{:?}", e)
    //     }
    // }
}

fn get_merged_ingest_objects(sender: mpsc::SyncSender<IngestObject>) -> () {
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
            ("after", EXPANSION_ERA_START)
        ])
            .send().expect("Eventually API call failed")
            .json::<EventuallyResponse>().expect("Eventually JSON decode failed");

        let len = response.len();

        for event in response.into_iter() {
            sender.send(IngestObject::EventuallyEvent(event)).unwrap();
        }

        if len < PAGE_SIZE {
            break;
        }

        page = page + 1;
    }
}
