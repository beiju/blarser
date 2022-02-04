use uuid::Uuid;

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
