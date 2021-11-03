use std::sync::mpsc;
use std::thread;
use serde::Serialize;

use crate::chronicler_schema::{ChroniclerItem, ChroniclerResponse};

pub const ENDPOINT_NAMES: [&str; 51] = [
    "player", "team", "stream", "idols", "tributes", "temporal", "tiebreakers", "sim",
    "globalevents", "offseasonsetup", "standings", "season", "league", "subleague", "division",
    "gamestatsheet", "teamstatsheet", "playerstatsheet", "seasonstatsheet", "bossfight",
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
    thread::spawn(move || chron_thread(sender, "https://api.sibr.dev/chronicler/v2/versions", &[
        ("type", &entity_type),
        ("after", &start)
    ]));
    receiver.into_iter().flatten()
}

pub fn entities(entity_type: &'static str, start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(2);
    thread::spawn(move || chron_thread(sender, "https://api.sibr.dev/chronicler/v2/entities", &[
        ("type", &entity_type),
        ("at", &start)
    ]));
    receiver.into_iter().flatten()
}

fn chron_thread<T: Serialize + ?Sized>(
    sender: mpsc::SyncSender<Vec<ChroniclerItem>>, url: &'static str, params: &T,
) -> () {
    let client = reqwest::blocking::Client::new();

    let mut page: Option<String> = None;

    loop {
        let request = client.get(url).query(params);

        let request = match page {
            Some(page) => request.query(&[("page", page)]),
            None => request
        };

        let response: ChroniclerResponse = request
            .send().expect("Chronicler API call failed")
            .json().expect("Chronicler JSON decode failed");

        sender.send(response.items).unwrap();

        page = match response.next_page {
            Some(p) => Some(p),
            None => return
        }
    }
}