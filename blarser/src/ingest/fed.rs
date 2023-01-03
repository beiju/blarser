use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use futures::{Stream, stream};
use fed::{FedEvent as FedEventBase, FedEventFlat};
use enum_flatten::EnumFlatten;
use log::info;

use crate::events::{AnyEvent, FedEvent};
use crate::ingest::error::IngestResult;
use crate::ingest::{GraphDebugHistory, StateGraph};
use crate::ingest::task::{DebugHistoryVersion, Ingest};

pub struct EventStreamItem {
    last_update_time: DateTime<Utc>,
    event: Option<AnyEvent>,
}

impl EventStreamItem {
    pub fn last_update_time(&self) -> DateTime<Utc> {
        self.last_update_time
    }

    pub fn event(&self) -> &Option<AnyEvent> {
        &self.event
    }

    pub fn into_event(self) -> Option<AnyEvent> {
        self.event
    }
}

pub fn get_fed_event_stream() -> impl Stream<Item=EventStreamItem> {
    // This is temporary, eventually it will be an HTTP call
    let fed_up_to_date_until = DateTime::parse_from_rfc3339(fed::EXPANSION_ERA_END)
        .expect("Couldn't parse hard-coded Blarser start time")
        .with_timezone(&Utc);

    let iter = fed::expansion_era_events()
        .map(move |event| EventStreamItem {
            last_update_time: fed_up_to_date_until,
            event: Some(FedEvent::new(event.unwrap()).into()),
        });

    stream::iter(iter)
}

#[derive(Debug, Default)]
pub struct TimedEventQueue {
    heap: BinaryHeap<TimedEventRecord>,
    next_index: u64,
}

impl TimedEventQueue {
    pub fn new() -> Self { Self::default() }

    pub fn push(&mut self, item: AnyEvent) {
        self.heap.push(TimedEventRecord {
            index: self.next_index,
            event: item,
        });
        self.next_index += 1;
    }

    pub fn extend(&mut self, items: impl IntoIterator<Item=AnyEvent>) {
        for item in items {
            self.push(item)
        }
    }

    pub fn peek(&self) -> Option<&AnyEvent> {
        self.heap.peek().map(|value| &value.event)
    }

    pub fn peek_with_index(&self) -> Option<(u64, &AnyEvent)> {
        self.heap.peek().map(|value| (value.index, &value.event))
    }

    pub fn pop(&mut self) -> Option<AnyEvent> {
        self.heap.pop().map(|value| value.event)
    }

    pub fn len(&self) -> usize { self.heap.len() }
}

impl<T: IntoIterator<Item=AnyEvent>> From<T> for TimedEventQueue {
    fn from(value: T) -> Self {
        let mut queue = TimedEventQueue::new();

        for item in value.into_iter() {
            queue.push(item);
        }

        queue
    }
}

#[derive(Debug)]
struct TimedEventRecord {
    index: u64,
    event: AnyEvent
}

impl Eq for TimedEventRecord {}

impl PartialEq<Self> for TimedEventRecord {
    fn eq(&self, other: &Self) -> bool {
        self.event.time().eq(&other.event.time()) && self.index.eq(&other.index)
    }
}

impl PartialOrd<Self> for TimedEventRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reversed order to turn the default max-heap behavior of BinaryHeap into min-heap.
        other.event.time().partial_cmp(&self.event.time())
            .and_then(|ord| {
                if ord == Ordering::Equal {
                    other.index.partial_cmp(&self.index)
                } else {
                    Some(ord)
                }
            })
    }
}

impl Ord for TimedEventRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed order to turn the default max-heap behavior of BinaryHeap into min-heap.
        other.event.time().cmp(&self.event.time())
            .then_with(|| other.index.cmp(&self.index))
    }
}

pub async fn get_timed_event_list(ingest: &mut Ingest, start_time: DateTime<Utc>) -> TimedEventQueue {
    let events = {
        let state = ingest.state.lock().unwrap();
        state.get_timed_events(start_time)
    };

    events.into()
}


pub async fn ingest_event(ingest: &mut Ingest, event: AnyEvent) -> IngestResult<Vec<AnyEvent>> {
    let mut history = ingest.debug_history.lock().await;
    let mut state = ingest.state.lock().unwrap();
    let mut new_timed_events = Vec::new();

    if let Some(predecessor) = event.generate_predecessor(&state) {
        info!("Event {event} has predecessor {predecessor}; ingesting that instead");
        new_timed_events.extend(ingest_event_internal(&mut state, predecessor, &mut history)?);
        // The original event becomes a timed event. Crucially, it gets inserted *after* the
        // successors of its predecessor.
        new_timed_events.push(event);
    } else {
        new_timed_events.extend(ingest_event_internal(&mut state, event, &mut history)?);
    }


    Ok(new_timed_events)
}

fn ingest_event_internal(
    state: &mut StateGraph,
    event: AnyEvent,
    history: &mut GraphDebugHistory,
) -> IngestResult<Vec<AnyEvent>> {
    let mut new_timed_events = Vec::new();

    info!("Ingesting event {event}");
    new_timed_events.extend(event.generate_successors(&state));
    let event_time = event.time();
    for effect in event.into_effects(&state) {
        let ty = effect.entity_type();
        for id in state.ids_for(&effect) {
            info!("Applying {effect} to {ty} {id}");
            let graph = state.entity_graph_mut(ty, id)
                .expect("Tried to apply event to entity that does not exist");
            graph.apply_effect(&effect, event_time);
            history.push(&(effect.entity_type(), id), DebugHistoryVersion {
                event_human_name: format!("After applying {effect}"),
                time: event_time,
                tree: graph.get_debug_tree(),
                queued_for_update: None,
                currently_updating: None,
                queued_for_delete: None,
            });
        }
    }

    Ok(new_timed_events)
}

// fn blarser_event_from_fed_event(fed_event: FedEvent) -> Option<AnyEvent> {
//     match fed_event.flat() {
//         _ => { todo!() }
//     }
//     // Some(match fed_event.flatten() {
//     //     FedEventFlat::BeingSpeech { .. } => { return None; }
//     //     FedEventFlat::LetsGo { game, .. } => {
//     //         events::LetsGo {
//     //             time: fed_event.created,
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             season: fed_event.season,
//     //             home_team: game.home_team,
//     //             away_team: game.away_team,
//     //         }.into()
//     //     }
//     //     FedEventData::PlayBall { game, .. } => {
//     //         events::PlayBall {
//     //             time: fed_event.created,
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             home_team: game.home_team,
//     //             away_team: game.away_team,
//     //         }.into()
//     //     }
//     //     FedEventData::HalfInningStart { game, top_of_inning, inning, .. } => {
//     //         events::HalfInning {
//     //             time: fed_event.created,
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             top_of_inning,
//     //             inning,
//     //             home_team: game.home_team,
//     //             away_team: game.away_team,
//     //         }.into()
//     //     }
//     //     FedEventData::BatterUp { game, batter_name, .. } => {
//     //         events::BatterUp {
//     //             time: fed_event.created,
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             batter_name: batter_name.clone(),
//     //         }.into()
//     //     }
//     //     FedEventData::SuperyummyGameStart { game, toggle, .. } => {
//     //         events::TogglePerforming {
//     //             time: fed_event.created,
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             player_id: toggle.player_id,
//     //             source_mod: "SUPERYUMMY".to_string(),
//     //             is_overperforming: toggle.is_overperforming,
//     //         }.into()
//     //     }
//     //     FedEventData::EchoedSuperyummyGameStart { .. } => { todo!() }
//     //     FedEventData::Ball { game, .. } => {
//     //         events::Ball {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::FoulBall { game, .. } => {
//     //         events::FoulBall {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::StrikeSwinging { game, .. } |
//     //     FedEventData::StrikeLooking { game, .. } |
//     //     FedEventData::StrikeFlinching { game, .. } => {
//     //         events::Strike {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     // TODO Separate these by whether someone can advance on them
//     //     FedEventData::StrikeoutLooking { game, .. } |
//     //     FedEventData::StrikeoutSwinging { game, .. } |
//     //     FedEventData::CharmStrikeout { game, .. } => {
//     //         events::Strikeout {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::Flyout { game, scores, .. } |
//     //     FedEventData::GroundOut { game, scores, .. } => {
//     //         events::CaughtOut {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: Some(scores),
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::FieldersChoice { game, .. } => {
//     //         events::FieldersChoice {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::DoublePlay { .. } => { todo!() }
//     //     FedEventData::Hit { game, num_bases, scores, batter_id, .. } => {
//     //         events::Hit {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: Some(scores),
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //             batter_id,
//     //             to_base: Base::try_from(num_bases)
//     //                 .expect("Invalid num_bases in Hit event"),
//     //         }.into()
//     //     }
//     //     FedEventData::HomeRun { game, num_runs, batter_id, .. } => {
//     //         events::HomeRun {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //             batter_id,
//     //             num_runs,
//     //         }.into()
//     //     }
//     //     FedEventData::StolenBase { game, base_stolen, runner_id, runner_name, free_refill, .. } => {
//     //         events::StolenBase {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //             to_base: Base::try_from(base_stolen)
//     //                 .expect("Invalid base_stolen in StolenBase event"),
//     //             runner_id,
//     //             runner_name,
//     //             free_refill,
//     //         }.into()
//     //     }
//     //     FedEventData::CaughtStealing { game, base_stolen, .. } => {
//     //         events::CaughtStealing {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //             to_base: Base::try_from(base_stolen)
//     //                 .expect("Invalid base_stolen in StolenBase event"),
//     //         }.into()
//     //     }
//     //     FedEventData::Walk { game, .. } => {
//     //         events::Walk {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::InningEnd { game, .. } => {
//     //         events::InningEnd {
//     //             game_update: GameUpdate {
//     //                 game_id: game.game_id,
//     //                 play: game.play,
//     //                 scores: None,
//     //                 description,
//     //             },
//     //             time: fed_event.created,
//     //         }.into()
//     //     }
//     //     FedEventData::StrikeZapped { .. } => { todo!() }
//     //     FedEventData::PeanutFlavorText { .. } => { todo!() }
//     //     FedEventData::GameEnd { .. } => { todo!() }
//     //     FedEventData::MildPitch { .. } => { todo!() }
//     //     FedEventData::MildPitchWalk { .. } => { todo!() }
//     //     FedEventData::CoffeeBean { .. } => { todo!() }
//     //     FedEventData::BecameMagmatic { .. } => { todo!() }
//     //     FedEventData::Blooddrain { .. } => { todo!() }
//     //     FedEventData::SpecialBlooddrain { .. } => { todo!() }
//     //     FedEventData::PlayerModExpires { .. } => { todo!() }
//     //     FedEventData::TeamModExpires { .. } => { todo!() }
//     //     FedEventData::BirdsCircle { .. } => { todo!() }
//     //     FedEventData::AmbushedByCrows { .. } => { todo!() }
//     //     FedEventData::Sun2SetWin { .. } => { todo!() }
//     //     FedEventData::BlackHoleSwallowedWin { .. } => { todo!() }
//     //     FedEventData::Sun2 { .. } => { todo!() }
//     //     FedEventData::BlackHole { .. } => { todo!() }
//     //     FedEventData::TeamDidShame { .. } => { todo!() }
//     //     FedEventData::TeamWasShamed { .. } => { todo!() }
//     //     FedEventData::CharmWalk { .. } => { todo!() }
//     //     FedEventData::GainFreeRefill { .. } => { todo!() }
//     //     FedEventData::AllergicReaction { .. } => { todo!() }
//     //     FedEventData::PerkUp { .. } => { todo!() }
//     //     FedEventData::Feedback { .. } => { todo!() }
//     //     FedEventData::BestowReverberating { .. } => { todo!() }
//     //     FedEventData::Reverb { .. } => { todo!() }
//     //     FedEventData::TarotReading { .. } => { todo!() }
//     //     FedEventData::TarotReadingAddedMod { .. } => { todo!() }
//     //     FedEventData::TeamEnteredPartyTime { .. } => { todo!() }
//     //     FedEventData::BecomeTripleThreat { .. } => { todo!() }
//     //     FedEventData::UnderOver { .. } => { todo!() }
//     //     FedEventData::OverUnder { .. } => { todo!() }
//     //     FedEventData::TasteTheInfinite { .. } => { todo!() }
//     //     FedEventData::BatterSkipped { .. } => { todo!() }
//     //     FedEventData::FeedbackBlocked { .. } => { todo!() }
//     //     FedEventData::FlagPlanted { .. } => { todo!() }
//     //     FedEventData::EmergencyAlert { .. } => { todo!() }
//     //     FedEventData::TeamJoinedILB { .. } => { todo!() }
//     //     FedEventData::FloodingSwept { .. } => { todo!() }
//     //     FedEventData::ReturnFromElsewhere { .. } => { todo!() }
//     //     FedEventData::Incineration { .. } => { todo!() }
//     //     FedEventData::PitcherChange { .. } => { todo!() }
//     //     FedEventData::Party { .. } => { todo!() }
//     //     FedEventData::PlayerHatched { .. } => { todo!() }
//     //     FedEventData::PostseasonBirth { .. } => { todo!() }
//     //     FedEventData::FinalStandings { .. } => { todo!() }
//     //     FedEventData::TeamLeftPartyTimeForPostseason { .. } => { todo!() }
//     //     FedEventData::EarnedPostseasonSlot { .. } => { todo!() }
//     //     FedEventData::PostseasonAdvance { .. } => { todo!() }
//     //     FedEventData::PostseasonEliminated { .. } => { todo!() }
//     //     FedEventData::PlayerBoosted { .. } => { todo!() }
//     //     FedEventData::TeamWonInternetSeries { .. } => { todo!() }
//     //     FedEventData::BottomDwellers { .. } => { todo!() }
//     //     FedEventData::WillReceived { .. } => { todo!() }
//     //     FedEventData::BlessingWon { .. } => { todo!() }
//     //     FedEventData::EarlbirdsAdded { .. } => { todo!() }
//     //     FedEventData::DecreePassed { .. } => { todo!() }
//     //     FedEventData::PlayerJoinedILB { .. } => { todo!() }
//     //     FedEventData::PlayerPermittedToStay { .. } => { todo!() }
//     //     FedEventData::FireproofIncineration { .. } => { todo!() }
//     //     FedEventData::LineupSorted { .. } => { todo!() }
//     //     FedEventData::EarlbirdsRemoved { .. } => { todo!() }
//     //     FedEventData::Undersea { .. } => { todo!() }
//     //     FedEventData::RenovationBuilt { .. } => { todo!() }
//     //     FedEventData::LateToThePartyAdded { .. } => { todo!() }
//     //     FedEventData::PeanutMister { .. } => { todo!() }
//     //     FedEventData::PlayerNamedMvp { .. } => { todo!() }
//     //     FedEventData::LateToThePartyRemoved { .. } => { todo!() }
//     //     FedEventData::BirdsUnshell { .. } => { todo!() }
//     //     FedEventData::ReplaceReturnedPlayerFromShadows { .. } => { todo!() }
//     //     FedEventData::PlayerCalledBackToHall { .. } => { todo!() }
//     //     FedEventData::TeamUsedFreeWill { .. } => { todo!() }
//     //     FedEventData::PlayerLostMod { .. } => { todo!() }
//     //     FedEventData::InvestigationMessage { .. } => { todo!() }
//     //     FedEventData::HighPressure { .. } => { todo!() }
//     //     FedEventData::PlayerPulledThroughRift { .. } => { todo!() }
//     //     FedEventData::PlayerLocalized { .. } => { todo!() }
//     //     FedEventData::Echo { .. } => { todo!() }
//     //     FedEventData::SolarPanelsAwait { .. } => { todo!() }
//     //     FedEventData::EchoIntoStatic { .. } => { todo!() }
//     //     FedEventData::Psychoacoustics { .. } => { todo!() }
//     //     FedEventData::EchoReceiver { .. } => { todo!() }
//     //     FedEventData::ConsumerAttack { .. } => { todo!() }
//     //     FedEventData::TeamGainedFreeWill { .. } => { todo!() }
//     //     FedEventData::Tidings { .. } => { todo!() }
//     //     FedEventData::HomebodyGameStart { .. } => { todo!() }
//     //     FedEventData::SalmonSwim { .. } => { todo!() }
//     //     FedEventData::HitByPitch { .. } => { todo!() }
//     //     FedEventData::SolarPanelsActivate { .. } => { todo!() }
//     //     FedEventData::RunsOverflowing { .. } => { todo!() }
//     //     FedEventData::Middling { .. } => { todo!() }
//     //     FedEventData::EnterCrimeScene { .. } => { todo!() }
//     //     FedEventData::ReturnFromInvestigation { .. } => { todo!() }
//     //     FedEventData::InvestigationConcluded { .. } => { todo!() }
//     //     FedEventData::GrindRail { .. } => { todo!() }
//     //     FedEventData::EnterSecretBase { .. } => { todo!() }
//     //     FedEventData::ExitSecretBase { .. } => { todo!() }
//     //     FedEventData::EchoChamber { .. } => { todo!() }
//     //     FedEventData::Roam { .. } => { todo!() }
//     // })
//     todo!()
// }