use std::sync::mpsc;
use std::thread;
use bincode;
use futures::{Stream, stream, StreamExt};
use log::{info, warn};

use crate::api::chronicler_schema::{ChroniclerItem, ChroniclerResponse, ChroniclerGameUpdate, ChroniclerGameUpdatesResponse, ChroniclerGamesResponse};

// This list comes directly from
// https://github.com/xSke/Chronicler/blob/main/SIBR.Storage.Data/Models/UpdateType.cs
//noinspection SpellCheckingInspection
pub const ENDPOINT_NAMES: [&str; 45] = [
    "player",
    "team",
    // Completely covered by "league", "temporal", "sim", and games (handled separately). See
    // https://discord.com/channels/738107179294523402/759177439745671169/918236757189873705
    // "stream",
    // Not offered in chron v2, handled separately
    // "game",
    "idols",
    // Peanut tributes to hall of flame players, not related to anything that happens in the sim
    // "tributes",
    "temporal",
    "tiebreakers",
    "sim",
    // This is the ticker, not connected to the sim at all
    // "globalevents",
    "offseasonsetup",
    "standings",
    "season",
    "league",
    "subleague",
    "division",
    // These 3 endpoints have too much data, and I don't expect them to be useful for seasons
    // where the feed exists. I may turn them back on if I ever get to parsing Discipline.
    // "gamestatsheet",
    // "teamstatsheet",
    // "playerstatsheet",
    "seasonstatsheet",
    "bossfight",
    "offseasonrecap",
    "bonusresult",
    "decreeresult",
    "eventresult",
    "playoffs",
    "playoffround",
    "playoffmatchup",
    "tournament",
    "stadium",
    "renovationprogress",
    "teamelectionstats",
    "item",
    "communitychestprogress",
    "giftprogress",
    "shopsetup",
    "sunsun",
    // This is (a) way too much data and (b) not at all useful for parsing
    // "librarystory",
    "vault",
    "risingstars",
    "fuelprogress",
    "nullified",
    "attributes",
    "fanart",
    "glossarywords",
    "library",
    "sponsordata",
    "stadiumprefabs",
    "feedseasonlist",
    "thebeat",
    "thebook",
    "championcallout",
    "dayssincelastincineration",
    // Payouts for champion bets. This is probably related to the sim but we don't know how
    // "availablechampionbets",
];

pub fn versions(entity_type: &'static str, start: &'static str) -> impl Stream<Item=ChroniclerItem> {
    chronicler_pages("versions", entity_type, start)
        .flat_map(|vec| stream::iter(vec.into_iter()))
}

pub fn entities(entity_type: &'static str, start: &'static str) -> impl Stream<Item=ChroniclerItem> {
    chronicler_pages("entities", entity_type, start)
        .flat_map(|vec| stream::iter(vec.into_iter()))
}

pub fn game_updates_or_schedule(schedule: bool, start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    let (sender, receiver) = mpsc::sync_channel(32);
    thread::spawn(move || game_updates_thread(sender, schedule, start));
    receiver
        .into_iter()
        .flatten()
        .map(|game| {
            ChroniclerItem {
                entity_id: game.game_id,
                valid_from: game.timestamp,
                valid_to: None,
                data: game.data,
            }
        })
}

pub fn game_updates(start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    game_updates_or_schedule(false, start)
}

pub fn schedule(start: &'static str) -> impl Iterator<Item=ChroniclerItem> {
    game_updates_or_schedule(true, start)
}

struct ChronState {
    pub page: Option<String>,
    pub stop: bool,
    pub cache: sled::Db,
    pub client: reqwest::Client,
}

fn chronicler_pages(endpoint: &'static str,
                    entity_type: &'static str,
                    start: &'static str) -> impl Stream<Item=Vec<ChroniclerItem>> {
    let start_state = ChronState {
        page: None,
        stop: false,
        cache: sled::open("http_cache/chron/".to_owned() + endpoint + "/" + entity_type).unwrap(),
        client: reqwest::Client::new(),
    };

    stream::unfold(start_state, move |state| async move {
        if state.stop {
            None
        } else {
            Some(chronicler_page(start, endpoint, entity_type, state).await)
        }
    })
}

async fn chronicler_page(start: &'static str,
                         endpoint: &'static str,
                         entity_type: &'static str,
                         state: ChronState) -> (Vec<ChroniclerItem>, ChronState) {
    let request = state.client
        .get("https://api.sibr.dev/chronicler/v2/".to_owned() + endpoint)
        .query(&[("type", &entity_type)]);

    let request = match endpoint {
        "entities" => request.query(&[("at", &start)]),
        "versions" => request.query(&[("after", &start)]),
        _ => panic!("Unexpected endpoint: {}", endpoint)
    };

    let request = match state.page {
        Some(page) => request.query(&[("page", &page)]),
        None => request
    };

    let request = request.build().unwrap();

    let cache_key = request.url().to_string();
    let response = match state.cache.get(&cache_key).unwrap() {
        Some(text) => bincode::deserialize(&text).unwrap(),
        None => {
            info!("Fetching chron {} page of type {} from network", endpoint, entity_type);

            let text = state.client
                .execute(request).await
                .expect("Chronicler API call failed")
                .text().await
                .expect("Chronicler text decode failed");

            state.cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

            text
        }
    };

    let response: ChroniclerResponse = serde_json::from_str(&response).unwrap();

    let stop = response.next_page.is_none();
    (
        response.items,
        ChronState {
            page: response.next_page,
            stop,
            cache: state.cache,
            client: state.client,
        }
    )
}

fn game_updates_thread(sender: mpsc::SyncSender<Vec<ChroniclerGameUpdate>>,
                       schedule: bool,
                       start: &'static str) {
    let client = reqwest::blocking::Client::new();

    let mut page: Option<String> = None;

    let request_type = if schedule { "schedule" } else { "updates" };

    let cache: sled::Db = sled::open("http_cache/game/".to_string() + request_type).unwrap();

    loop {
        let request = client
            .get("https://api.sibr.dev/chronicler/v1/games".to_string() + if schedule { "" } else { "/updates" })
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
                info!("Fetching game {} page from network", request_type);

                let text = client
                    .execute(request).expect("Chronicler API call failed")
                    .text().expect("Chronicler text decode failed");

                cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

                text
            }
        };

        let (response_data, next_page) = if schedule {
            let games_response: ChroniclerGamesResponse = serde_json::from_str(&response).unwrap();
            let games: Vec<_> = games_response.data.into_iter()
                .map(|item| {
                    let request = client
                        .get("https://api.sibr.dev/chronicler/v1/games/updates")
                        .query(&[("game", item.game_id.to_string())])
                        .query(&[("order", "asc")])
                        .query(&[("count", 1)])
                        .build().unwrap();

                    let cache_key = request.url().to_string();
                    let response = match cache.get(&cache_key).unwrap() {
                        Some(text) => bincode::deserialize(&text).unwrap(),
                        None => {
                            info!("Fetching latest update for game {} from network", item.game_id);

                            let text = client
                                .execute(request).expect("Chronicler API call failed")
                                .text().expect("Chronicler text decode failed");

                            cache.insert(&cache_key, bincode::serialize(&text).unwrap()).unwrap();

                            text
                        }
                    };

                    let response: ChroniclerGameUpdatesResponse = serde_json::from_str(&response).unwrap();

                    response.data.into_iter().next().unwrap()
                })
                .collect();

            (games, games_response.next_page)
        } else {
            let response: ChroniclerGameUpdatesResponse = serde_json::from_str(&response).unwrap();
            (response.data, response.next_page)
        };

        match sender.send(response_data) {
            Ok(_) => {}
            Err(err) => {
                warn!("Exiting game {} thread due to {:?}", request_type, err);
                return;
            }
        }

        page = match next_page {
            Some(p) => Some(p),
            None => return
        }
    }
}
