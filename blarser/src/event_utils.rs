use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};

pub fn get_one_id<'a>(tags: &'a [Uuid], field_name: &'static str) -> &'a Uuid {
    get_one_id_excluding(tags, field_name, None)
}

pub fn get_one_id_excluding<'a>(tags: &'a [Uuid], field_name: &'static str, excluding: Option<&'a Uuid>) -> &'a Uuid {
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


pub fn separate_scoring_events<'a>(siblings: &'a Vec<EventuallyEvent>, hitter_id: &'a Uuid) -> (Vec<&'a Uuid>, Vec<&'a EventuallyEvent>) {
    // The first event is never a scoring event, and it mixes up the rest of the logic because the
    // "hit" or "walk" event type is reused
    let (first, rest) = siblings.split_first()
        .expect("Event's siblings array is empty");
    let mut scores = Vec::new();
    let mut others = vec![first];

    for event in rest {
        if event.r#type == EventType::Hit || event.r#type == EventType::Walk {
            scores.push(get_one_id_excluding(&event.player_tags, "playerTags", Some(hitter_id)));
        } else if event.r#type != EventType::RunsScored {
            others.push(event);
        }
    }

    (scores, others)
}
