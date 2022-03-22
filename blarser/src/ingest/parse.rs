use diesel::QueryResult;
use uuid::Uuid;
use crate::api::{EventType, EventuallyEvent};
use crate::events::{AnyEvent, HalfInning, LetsGo, PlayBall};
use crate::state::StateInterface;

pub fn parse_feed_event(feed_event: EventuallyEvent, state: &StateInterface) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
    match feed_event.r#type {
        EventType::LetsGo => LetsGo::parse(feed_event),
        EventType::PlayBall => PlayBall::parse(feed_event),
        EventType::HalfInning => HalfInning::parse(feed_event, state),
        // EventType::PitcherChange => PitcherChange::parse(feed_event),
        // EventType::StolenBase => StolenBase::parse(feed_event),
        // EventType::Walk => Walk::parse(feed_event),
        // EventType::Strikeout => Strikeout::parse(feed_event),
        // // It's easier to combine ground out and flyout types into one function
        // EventType::FlyOut => FieldingOut::Parse(feed_event, state),
        // EventType::GroundOut => FieldingOut::Parse(feed_event, state),
        // EventType::HomeRun => HomeRun::parse(feed_event),
        // EventType::Hit => Hit::parse(feed_event),
        // EventType::GameEnd => GameEnd::parse(feed_event),
        // EventType::BatterUp => BatterUp::parse(feed_event),
        // EventType::Strike => Strike::parse(feed_event),
        // EventType::Ball => Ball::parse(feed_event),
        // EventType::FoulBall => FoulBall::parse(feed_event),
        // EventType::InningEnd => InningEnd::parse(feed_event),
        // EventType::BatterSkipped => BatterSkipped::parse(feed_event),
        // EventType::PeanutFlavorText => FlavorText::parse(feed_event),
        // EventType::PlayerStatReroll => PlayerStatReroll::parse(feed_event),
        // EventType::WinCollectedRegular => WinCollectedRegular::parse(feed_event),
        // EventType::GameOver => GameOver::parse(feed_event),
        // EventType::StormWarning => StormWarning::parse(feed_event),
        // EventType::Snowflakes => Snowflakes::parse(feed_event),
        // EventType::ModExpires => ModExpires::parse(feed_event),
        // EventType::ShamingRun => ShamingRun::parse(feed_event),
        // EventType::TeamWasShamed => TeamWasShamed::parse(feed_event),
        // EventType::TeamDidShame => TeamDidShame::parse(feed_event),
        _ => todo!(),
    }
}