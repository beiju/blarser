use std::sync::mpsc;
use std::thread;
use bincode;
use itertools::Itertools;
use log::{debug, warn};

use crate::api::chronicler_schema::{ChroniclerItem, ChroniclerResponse, ChroniclerV1Game, ChroniclerV1Response};

pub const ENDPOINT_NAMES: [&str; 40] = [
    "player", "team", "tiebreakers", "sim",
    "offseasonsetup", "standings", "season", "league", "subleague", "division",
    "bossfight",
    "offseasonrecap", "bonusresult", "decreeresult", "eventresult", "playoffs", "playoffround",
    "playoffmatchup", "tournament", "stadium", "renovationprogress", "teamelectionstats", "item",
    "communitychestprogress", "shopsetup", "sunsun", "vault",
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

pub fn games(start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(32);
    thread::spawn(move || chron_v1_thread(sender, "games/updates", start));
    receiver
        .into_iter()
        .flatten()
        .map(|game| {
            ChroniclerItem {
                entity_id: game.game_id,
                hash: game.hash,
                valid_from: game.timestamp,
                valid_to: Some(game.timestamp),
                data: game.data
            }
        })
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
                debug!("Fetching chron {} page of type {} from network", endpoint, entity_type);

                let text = client
                    .execute(request).expect("Chronicler API call failed")
                    .text().expect("Chronicler text decode failed");

                cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

                text
            }
        };

        let response: ChroniclerResponse = serde_json::from_str(&response).unwrap();

        match sender.send(response.items) {
            Ok(_) => {}
            Err(err) => {
                warn!("Exiting chron {} thread due to {:?}", entity_type, err);
                return;
            }
        }

        page = match response.next_page {
            Some(p) => Some(p),
            None => return
        }
    }
}

fn chron_v1_thread(sender: mpsc::SyncSender<Vec<ChroniclerV1Game>>,
                   endpoint: &'static str,
                   start: &'static str) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page: Option<String> = None;

    let cache: sled::Db = sled::open("http_cache/chron_v1/".to_owned() + endpoint).unwrap();

    loop {
        let request = client
            .get("https://api.sibr.dev/chronicler/v1/".to_owned() + endpoint)
            .query(&[("after", &start)]);

        let request = match page {
            Some(page) => request.query(&[("page", &page)]),
            None => request
        };

        let request = request.build().unwrap();

        let cache_key = request.url().to_string();
        let response = match cache.get(&cache_key).unwrap() {
            Some(text) => bincode::deserialize(&text).unwrap(),
            None => {
                debug!("Fetching chron v1 {} page of type from network", endpoint);

                let text = client
                    .execute(request).expect("Chronicler API call failed")
                    .text().expect("Chronicler text decode failed");

                cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

                text
            }
        };

        let response: ChroniclerV1Response = serde_json::from_str(&response).unwrap();

        match sender.send(response.data) {
            Ok(_) => {}
            Err(err) => {
                warn!("Exiting chron v1 {} thread due to {:?}", endpoint, err);
                return;
            }
        }

        page = match response.next_page {
            Some(p) => Some(p),
            None => return
        }
    }
}
