use diesel::QueryResult;
use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};
use crate::events::{self, AnyEvent};
use crate::state::StateInterface;

pub fn parse_feed_event(feed_event: &EventuallyEvent, state: &StateInterface) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
    match feed_event.r#type {
        EventType::LetsGo => events::LetsGo::parse(feed_event),
        EventType::PlayBall => events::PlayBall::parse(feed_event),
        EventType::HalfInning => events::HalfInning::parse(feed_event, state),
        // EventType::PitcherChange => events::PitcherChange::parse(feed_event),
        // EventType::StolenBase => events::StolenBase::parse(feed_event),
        // EventType::Walk => events::Walk::parse(feed_event),
        // EventType::Strikeout => events::Strikeout::parse(feed_event),
        // It's easier to combine ground out and flyout types into one function
        EventType::FlyOut => events::parse_fielding_out(feed_event),
        EventType::GroundOut => events::parse_fielding_out(feed_event),
        // EventType::HomeRun => events::HomeRun::parse(feed_event),
        EventType::Hit => events::Hit::parse(feed_event, state),
        // EventType::GameEnd => events::GameEnd::parse(feed_event),
        EventType::BatterUp => events::BatterUp::parse(feed_event),
        EventType::Strike => events::Strike::parse(feed_event),
        EventType::Ball => events::Ball::parse(feed_event),
        EventType::FoulBall => events::FoulBall::parse(feed_event),
        // EventType::InningEnd => events::InningEnd::parse(feed_event),
        // EventType::BatterSkipped => events::BatterSkipped::parse(feed_event),
        // EventType::PeanutFlavorText => events::FlavorText::parse(feed_event),
        // EventType::PlayerStatReroll => events::PlayerStatReroll::parse(feed_event),
        // EventType::WinCollectedRegular => events::WinCollectedRegular::parse(feed_event),
        // EventType::GameOver => events::GameOver::parse(feed_event),
        EventType::StormWarning => events::StormWarning::parse(feed_event),
        // EventType::Snowflakes => events::Snowflakes::parse(feed_event),
        // EventType::ModExpires => events::ModExpires::parse(feed_event),
        // EventType::ShamingRun => events::ShamingRun::parse(feed_event),
        // EventType::TeamWasShamed => events::TeamWasShamed::parse(feed_event),
        // EventType::TeamDidShame => events::TeamDidShame::parse(feed_event),
        _ => todo!(),
    }
}