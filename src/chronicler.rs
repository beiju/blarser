use std::sync::mpsc;
use std::thread;
use bincode;

use crate::chronicler_schema::{ChroniclerItem, ChroniclerResponse};

pub const ENDPOINT_NAMES: [&str; 44] = [
    "player", "team", "temporal", "tiebreakers", "sim",
    "globalevents", "offseasonsetup", "standings", "season", "league", "subleague", "division",
    "bossfight",
    "offseasonrecap", "bonusresult", "decreeresult", "eventresult", "playoffs", "playoffround",
    "playoffmatchup", "tournament", "stadium", "renovationprogress", "teamelectionstats", "item",
    "communitychestprogress", "giftprogress", "shopsetup", "sunsun", "librarystory", "vault",
    "risingstars", "fuelprogress", "nullified", "fanart", "glossarywords", "library", "sponsordata",
    "stadiumprefabs", "feedseasonlist", "thebeat", "thebook", "championcallout",
    "dayssincelastincineration"
];

pub fn versions(entity_type: &'static str, start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    // This sends Vec<ChroniclerItem>, rather than just ChroniclerItem, so the sync_channel's
    // internal buffer can be used for prefetching the next page.
    let (sender, receiver) = mpsc::sync_channel(2);
    thread::spawn(move || chron_thread(sender, "versions", entity_type, start));
    receiver.into_iter().flatten()
}

pub fn entities(entity_type: &'static str, start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(32);
    thread::spawn(move || chron_thread(sender, "entities", entity_type, start));
    receiver.into_iter().flatten()
}

fn chron_thread(sender: mpsc::SyncSender<Vec<ChroniclerItem>>,
                endpoint: &'static str,
                entity_type: &'static str,
                start: &'static str) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page: Option<String> = None;

    let cache: sled::Db = sled::open("http_cache/chron/".to_owned() + endpoint + "/" + entity_type).unwrap();

    loop {
        let request = client
            .get("https://api.sibr.dev/chronicler/v2/".to_owned() + endpoint)
            .query(&[("type", &entity_type)]);

        let request = match endpoint {
            "entities" => request.query(&[("at", &start)]),
            "versions" => request.query(&[("after", &start)]),
            _ => panic!("Unexpected endpoint: {}", endpoint)
        };

        let request = match page {
            Some(page) => request.query(&[("page", &page)]),
            None => request
        };

        let request = request.build().unwrap();

        let cache_key = request.url().to_string();
        let response = match cache.get(&cache_key).unwrap() {
            Some(text) => bincode::deserialize(&text).unwrap(),
            None => {
                println!("Fetching chron {} page of type {} from network", endpoint, entity_type);

                let text = client
                    .execute(request).expect("Chronicler API call failed")
                    .text().expect("Chronicler text decode failed");

                cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

                text
            }
        };

        let response: ChroniclerResponse = serde_json::from_str(&response).unwrap();

        match sender.send(response.items) {
            Ok(_) => { },
            Err(err) => {
                println!("Exiting chron {} thread due to {:?}", entity_type, err);
                return;
            }
        }

        // Apparently some endpoints just send the same page

        page = match response.next_page {
            Some(p) => Some(p),
            None => return
        }
    }
}