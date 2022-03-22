mod feed_event;
mod timed_event;
mod game_update;
mod lets_go;
mod play_ball;
mod half_inning;

pub use lets_go::LetsGo;
pub use play_ball::PlayBall;
pub use half_inning::HalfInning;

use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::entity::{Entity};


#[enum_dispatch]
pub trait EventTrait {
    fn time(&self) -> DateTime<Utc>;

    fn forward(&self, entity: Entity, aux: &EventAux) -> Entity;
    fn reverse(&self, entity: Entity, aux: &EventAux) -> Entity;
}

#[derive(Serialize, Deserialize)]
#[enum_dispatch(EventTrait)]
pub enum Event {
    LetsGo(LetsGo),
    PlayBall(PlayBall),
    HalfInning(HalfInning),
}

#[derive(PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum EventAux {
    None,
    Pitchers {
        home: (Uuid, String),
        away: (Uuid, String)
    }
}

// impl Event {
//     pub fn apply(&self, state: &impl StateInterface) {
//         match self {
//             Event::Start => {
//                 panic!("Can't re-apply a Start event!")
//             }
//             Event::Feed(feed_event) => {
//                 feed_event.apply(state)
//             }
//             Event::Timed(timed_event) => {
//                 timed_event.apply(state)
//             }
//             Event::Manual(_) => {
//                 todo!()
//             }
//         }
//     }
//
//     pub fn type_str(&self) -> String {
//         match self {
//             Event::Start => { "Start".to_string() }
//             Event::Feed(e) => { format!("{:?}", e.r#type) }
//             Event::Timed(t) => { format!("{:?}", t.event_type) }
//             Event::Manual(_) => { "Manual".to_string() }
//         }
//     }
// }
//
// impl Display for Event {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Event::Start => { write!(f, "Start") }
//             Event::Feed(event) => {
//                 if event.metadata.siblings.is_empty() {
//                     write!(f, "{}", event.description)
//                 } else {
//                     write!(f, "{}", event.metadata.siblings.iter()
//                         .map(|event| &event.description)
//                         .join("\n"))
//                 }
//             }
//             Event::Timed(event) => {
//                 write!(f, "{}", event.event_type.description())
//             }
//             Event::Manual(event) => {
//                 write!(f, "{}", event.description())
//             }
//         }
//     }
// }