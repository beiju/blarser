use rocket::error;
use serde::Deserialize;
use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};
use crate::event_utils;

fn change_game(event: &EventuallyEvent) -> (&'static str, Option<Uuid>) {
    ("game", Some(*event_utils::get_one_id(&event.game_tags, "gameTags")))
}

fn change_player(event: &EventuallyEvent) -> (&'static str, Option<Uuid>) {
    // Sometimes fielding outs don't say what player they're for, which is *very* annoying
    if event.player_tags.is_empty() {
        ("player", None)
    } else {
        ("player", Some(*event_utils::get_one_id(&event.player_tags, "playerTags")))
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LetsGoMetadata {
    pub home: Uuid,
    pub away: Uuid,
}


pub fn changes_for_event(event: &EventuallyEvent) -> Vec<(&'static str, Option<Uuid>)> {
    match event.r#type {
        EventType::LetsGo => {
            // LetsGo events change the active pitcher on Team entities
            let metadata: LetsGoMetadata = serde_json::from_value(event.metadata.other.clone())
                .expect("Couldn't parse metadata for LetsGo event");
            vec![change_game(event), ("team", Some(metadata.home)), ("team", Some(metadata.away))]
        }
        EventType::StormWarning => vec![change_game(event)],
        EventType::PlayBall => vec![change_game(event)],
        EventType::HalfInning => vec![change_game(event)],
        EventType::BatterUp => vec![change_game(event)], // Haunting not implemented yet
        EventType::Strike => vec![change_game(event)],
        EventType::Ball => vec![change_game(event)],
        EventType::FoulBall => vec![change_game(event)], // Filthiness not implemented yet
        // Outs and hits change the consecutiveHits property of players, as well as Spicy
        EventType::Strikeout => vec![change_game(event), change_player(event)],
        EventType::FlyOut => vec![change_game(event), change_player(event)],
        EventType::GroundOut => vec![change_game(event), change_player(event)],
        EventType::Hit => vec![change_game(event), change_player(event)],
        EventType::HomeRun => vec![change_game(event), change_player(event)],
        EventType::PlayerStatReroll => vec![change_player(event)],
        EventType::Snowflakes => vec![change_game(event), ("player", None)],
        EventType::StolenBase => vec![change_game(event)],
        EventType::Walk => vec![change_game(event)],
        EventType::InningEnd => vec![change_game(event)], // Losing triple threat not implemented yet
        EventType::BatterSkipped => vec![change_game(event)],
        EventType::PeanutFlavorText => vec![change_game(event)],
        unknown_type => {
            error!("Don't know changes for event {:?}", unknown_type);
            todo!()
        }
    }
}