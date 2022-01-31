use rocket::error;
use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};

fn get_one_id<'a>(tags: &'a [Uuid], field_name: &'static str) -> &'a Uuid {
    get_one_id_excluding(tags, field_name, None)
}

fn get_one_id_excluding<'a>(tags: &'a [Uuid], field_name: &'static str, excluding: Option<&'a Uuid>) -> &'a Uuid {
    match tags.len() {
        0 => {
            panic!("Expected exactly one element in {} but found none", field_name)
        }
        1 => {
            &tags[0]
        }
        2 => {
            if let Some(excluding) = excluding {
                if tags[0] == *excluding {
                    &tags[1]
                } else if tags[1] == *excluding {
                    &tags[0]
                } else {
                    panic!("Expected exactly one element in {}, excluding {}, but found two (neither excluded)", field_name, excluding)
                }
            } else {
                panic!("Expected exactly one element in {} but found 2", field_name)
            }
        }
        n => {
            panic!("Expected exactly one element in {} but found {}", field_name, n)
        }
    }
}

fn change_game(event: &EventuallyEvent) -> (&'static str, Option<Uuid>) {
    ("game", Some(*get_one_id(&event.game_tags, "gameTags")))
}

fn change_player(event: &EventuallyEvent) -> (&'static str, Option<Uuid>) {
    // Sometimes fielding outs don't say what player they're for, which is *very* annoying
    if event.player_tags.is_empty() {
        ("player", None)
    } else {
        ("player", Some(*get_one_id(&event.player_tags, "playerTags")))
    }
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