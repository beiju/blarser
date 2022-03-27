use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Copy, Clone, Serialize, Deserialize)]
#[repr(i64)]
pub enum Base {
    First = 0,
    Second = 1,
    Third = 2,
    Fourth = 3,
}

impl Base {
    pub fn name(&self) -> &'static str {
        match self {
            Base::First => { "first" }
            Base::Second => { "second" }
            Base::Third => { "third" }
            Base::Fourth => { "fourth" }
        }
    }

    pub fn from_string(base_name: &str) -> Self {
        match base_name {
            "first" => { Base::First }
            "second" => { Base::Second }
            "third" => { Base::Third }
            "fourth" => { Base::Fourth }
            _ => { panic!("Invalid base name {}", base_name) }
        }
    }

    pub fn from_hit(hit_name: &str) -> Self {
        match hit_name {
            "Single" => Base::First,
            "Double" => Base::Second,
            "Triple" => Base::Third,
            "Quadruple" => Base::Fourth,
            _ => panic!("Invalid hit type {}", hit_name)
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunnerAdvancement {
    pub runner_id: Uuid,
    pub from_base: i32,
    pub to_base: i32,
}