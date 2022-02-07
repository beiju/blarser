use rocket::error;
use serde::Deserialize;
use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};
use crate::event_utils;

fn change_game(event: &EventuallyEvent) -> (&'static str, Option<Uuid>) {
    ("game", Some(*event_utils::get_one_id(&event.game_tags, "gameTags")))
}

fn change_team_i(event: &EventuallyEvent, i: usize) -> (&'static str, Option<Uuid>) {
    ("team", Some(event.team_tags[i]))
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
        EventType::LetsGo => vec![change_game(event)],
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
        EventType::Snowflakes => changes_for_snowflakes(event),
        EventType::StolenBase => vec![change_game(event)],
        EventType::Walk => vec![change_game(event)],
        EventType::InningEnd => vec![change_game(event)], // Losing triple threat not implemented yet
        EventType::BatterSkipped => vec![change_game(event)],
        EventType::PeanutFlavorText => vec![change_game(event)],
        EventType::GameEnd => vec![change_game(event), ("standings", None), change_team_i(event, 0), change_team_i(event, 1)],
        EventType::WinCollectedRegular => vec![change_game(event)],
        EventType::GameOver => vec![change_game(event)],
        EventType::ModExpires => vec![change_player(event)],
        EventType::PitcherChange => vec![change_game(event), change_team_i(event, 0), change_team_i(event, 1)], // I presume this changes something in team
        unknown_type => {
            error!("Don't know changes for event {:?}", unknown_type);
            todo!()
        }
    }
}

fn changes_for_snowflakes(event: &EventuallyEvent) -> Vec<(&'static str, Option<Uuid>)> {
    let mut changes = vec![change_game(event)];
    for event in &event.metadata.siblings {
        if event.r#type == EventType::AddedMod {
            changes.push(change_player(event));
        }
    }

    changes
}