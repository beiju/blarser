use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use futures::{Stream, stream};
use fed::{FedEvent, FedEventData};
use log::info;
use crate::entity::Base;

use crate::events;
use crate::events::{Effect, AnyEvent, GameUpdate};
use crate::ingest::error::IngestResult;
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
            event: blarser_event_from_fed_event(event.unwrap()),
        });

    stream::iter(iter)
}

pub async fn get_timed_event_list(ingest: &mut Ingest, start_time: DateTime<Utc>) -> BinaryHeap<Reverse<AnyEvent>> {
    let events = {
        let state = ingest.state.lock().unwrap();
        state.get_timed_events(start_time)
    };

    BinaryHeap::from(events.into_iter().map(Reverse).collect::<Vec<_>>())
}


pub async fn ingest_event(ingest: &mut Ingest, event: AnyEvent) -> IngestResult<Vec<AnyEvent>> {
    let mut history = ingest.debug_history.lock().await;
    let event = Arc::new(event);
    let mut state = ingest.state.lock().unwrap();
    info!("Ingesting event {event}");
    let effects: Vec<Effect> = event.effects(&state);
    let mut new_timed_events = Vec::new();
    for effect in effects {
        for id in state.ids_for(&effect) {
            let graph = state.entity_graph_mut(effect.ty, id)
                .expect("Tried to apply event to entity that does not exist");
            info!("Applying {event} to {} {id} with {:?}", effect.ty, effect.extrapolated);
            let new_events = graph.apply_event(event.clone(), &effect.extrapolated)?;
            new_timed_events.extend(new_events);
            history.get_mut(&(effect.ty, id)).unwrap().versions.push(DebugHistoryVersion {
                event_human_name: format!("After applying {event}"),
                time: event.time(),
                tree: graph.get_debug_tree(),
                queued_for_update: None,
                currently_updating: None,
                queued_for_delete: None,
            });
        }
    }

    Ok(new_timed_events)
}

fn blarser_event_from_fed_event(fed_event: FedEvent) -> Option<AnyEvent> {
    // TODO Make a function to just get description
    let description = fed_event.clone().into_feed_event().description;
    Some(match fed_event.data {
        FedEventData::BeingSpeech { .. } => { return None; }
        FedEventData::LetsGo { game, .. } => {
            events::LetsGo {
                time: fed_event.created,
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
            }.into()
        }
        FedEventData::PlayBall { game, .. } => {
            events::PlayBall {
                time: fed_event.created,
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
            }.into()
        }
        FedEventData::HalfInningStart { game, .. } => {
            events::HalfInning {
                time: fed_event.created,
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
            }.into()
        }
        FedEventData::BatterUp { game, batter_name, ..  } => {
            events::BatterUp {
                time: fed_event.created,
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                batter_name: batter_name.clone(),
            }.into()
        }
        FedEventData::SuperyummyGameStart { game, .. } => {
            events::TogglePerforming {
                time: fed_event.created,
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                which_mod: "SUPERYUMMY".to_string(),
            }.into()
        }
        FedEventData::EchoedSuperyummyGameStart { .. } => { todo!() }
        FedEventData::Ball { game, .. } => {
            events::Ball {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
            }.into()
        }
        FedEventData::FoulBall { game, .. } => {
            events::FoulBall {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
            }.into()
        }
        FedEventData::StrikeSwinging { game, .. } |
        FedEventData::StrikeLooking { game, .. } |
        FedEventData::StrikeFlinching { game, .. }=> {
            events::Strike {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
            }.into()
        }
        FedEventData::StrikeoutLooking { game, .. } |
        FedEventData::StrikeoutSwinging { game, .. } |
        FedEventData::Flyout { game, .. } |
        FedEventData::GroundOut { game, .. } => {
            events::Out {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
            }.into()
        }
        FedEventData::FieldersChoice { .. } => { todo!() }
        FedEventData::DoublePlay { .. } => { todo!() }
        FedEventData::Hit { game, num_bases, .. } => {
            events::Hit {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
                to_base: Base::try_from(num_bases)
                    .expect("Invalid num_bases in Hit event"),
            }.into()
        }
        FedEventData::HomeRun { game, .. } => {
            events::HomeRun {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
            }.into()
        }
        FedEventData::StolenBase { game, base_stolen, .. } => {
            events::StolenBase {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
                to_base: Base::try_from(base_stolen)
                    .expect("Invalid base_stolen in StolenBase event"),
            }.into()
        }
        FedEventData::CaughtStealing { game, base_stolen, .. } => {
            events::CaughtStealing {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
                to_base: Base::try_from(base_stolen)
                    .expect("Invalid base_stolen in StolenBase event"),
            }.into()

        }
        FedEventData::Walk { game, .. } => {
            events::Walk {
                game_update: GameUpdate {
                    game_id: game.game_id,
                    play: game.play,
                    score: None,
                    description,
                },
                time: fed_event.created,
            }.into()
        }
        FedEventData::InningEnd { .. } => { todo!() }
        FedEventData::CharmStrikeout { .. } => { todo!() }
        FedEventData::StrikeZapped { .. } => { todo!() }
        FedEventData::PeanutFlavorText { .. } => { todo!() }
        FedEventData::GameEnd { .. } => { todo!() }
        FedEventData::MildPitch { .. } => { todo!() }
        FedEventData::MildPitchWalk { .. } => { todo!() }
        FedEventData::CoffeeBean { .. } => { todo!() }
        FedEventData::BecameMagmatic { .. } => { todo!() }
        FedEventData::Blooddrain { .. } => { todo!() }
        FedEventData::SpecialBlooddrain { .. } => { todo!() }
        FedEventData::PlayerModExpires { .. } => { todo!() }
        FedEventData::TeamModExpires { .. } => { todo!() }
        FedEventData::BirdsCircle { .. } => { todo!() }
        FedEventData::AmbushedByCrows { .. } => { todo!() }
        FedEventData::Sun2SetWin { .. } => { todo!() }
        FedEventData::BlackHoleSwallowedWin { .. } => { todo!() }
        FedEventData::Sun2 { .. } => { todo!() }
        FedEventData::BlackHole { .. } => { todo!() }
        FedEventData::TeamDidShame { .. } => { todo!() }
        FedEventData::TeamWasShamed { .. } => { todo!() }
        FedEventData::CharmWalk { .. } => { todo!() }
        FedEventData::GainFreeRefill { .. } => { todo!() }
        FedEventData::AllergicReaction { .. } => { todo!() }
        FedEventData::PerkUp { .. } => { todo!() }
        FedEventData::Feedback { .. } => { todo!() }
        FedEventData::BestowReverberating { .. } => { todo!() }
        FedEventData::Reverb { .. } => { todo!() }
        FedEventData::TarotReading { .. } => { todo!() }
        FedEventData::TarotReadingAddedMod { .. } => { todo!() }
        FedEventData::TeamEnteredPartyTime { .. } => { todo!() }
        FedEventData::BecomeTripleThreat { .. } => { todo!() }
        FedEventData::UnderOver { .. } => { todo!() }
        FedEventData::OverUnder { .. } => { todo!() }
        FedEventData::TasteTheInfinite { .. } => { todo!() }
        FedEventData::BatterSkipped { .. } => { todo!() }
        FedEventData::FeedbackBlocked { .. } => { todo!() }
        FedEventData::FlagPlanted { .. } => { todo!() }
        FedEventData::EmergencyAlert { .. } => { todo!() }
        FedEventData::TeamJoinedILB { .. } => { todo!() }
        FedEventData::FloodingSwept { .. } => { todo!() }
        FedEventData::ReturnFromElsewhere { .. } => { todo!() }
        FedEventData::Incineration { .. } => { todo!() }
        FedEventData::PitcherChange { .. } => { todo!() }
        FedEventData::Party { .. } => { todo!() }
        FedEventData::PlayerHatched { .. } => { todo!() }
        FedEventData::PostseasonBirth { .. } => { todo!() }
        FedEventData::FinalStandings { .. } => { todo!() }
        FedEventData::TeamLeftPartyTimeForPostseason { .. } => { todo!() }
        FedEventData::EarnedPostseasonSlot { .. } => { todo!() }
        FedEventData::PostseasonAdvance { .. } => { todo!() }
        FedEventData::PostseasonEliminated { .. } => { todo!() }
        FedEventData::PlayerBoosted { .. } => { todo!() }
        FedEventData::TeamWonInternetSeries { .. } => { todo!() }
        FedEventData::BottomDwellers { .. } => { todo!() }
        FedEventData::WillReceived { .. } => { todo!() }
        FedEventData::BlessingWon { .. } => { todo!() }
        FedEventData::EarlbirdsAdded { .. } => { todo!() }
        FedEventData::DecreePassed { .. } => { todo!() }
        FedEventData::PlayerJoinedILB { .. } => { todo!() }
        FedEventData::PlayerPermittedToStay { .. } => { todo!() }
        FedEventData::FireproofIncineration { .. } => { todo!() }
        FedEventData::LineupSorted { .. } => { todo!() }
        FedEventData::EarlbirdsRemoved { .. } => { todo!() }
        FedEventData::Undersea { .. } => { todo!() }
        FedEventData::RenovationBuilt { .. } => { todo!() }
        FedEventData::LateToThePartyAdded { .. } => { todo!() }
        FedEventData::PeanutMister { .. } => { todo!() }
        FedEventData::PlayerNamedMvp { .. } => { todo!() }
        FedEventData::LateToThePartyRemoved { .. } => { todo!() }
        FedEventData::BirdsUnshell { .. } => { todo!() }
        FedEventData::ReplaceReturnedPlayerFromShadows { .. } => { todo!() }
        FedEventData::PlayerCalledBackToHall { .. } => { todo!() }
        FedEventData::TeamUsedFreeWill { .. } => { todo!() }
        FedEventData::PlayerLostMod { .. } => { todo!() }
        FedEventData::InvestigationMessage { .. } => { todo!() }
        FedEventData::HighPressure { .. } => { todo!() }
        FedEventData::PlayerPulledThroughRift { .. } => { todo!() }
        FedEventData::PlayerLocalized { .. } => { todo!() }
        FedEventData::Echo { .. } => { todo!() }
        FedEventData::SolarPanelsAwait { .. } => { todo!() }
        FedEventData::EchoIntoStatic { .. } => { todo!() }
        FedEventData::Psychoacoustics { .. } => { todo!() }
        FedEventData::EchoReceiver { .. } => { todo!() }
        FedEventData::ConsumerAttack { .. } => { todo!() }
        FedEventData::TeamGainedFreeWill { .. } => { todo!() }
        FedEventData::Tidings { .. } => { todo!() }
        FedEventData::HomebodyGameStart { .. } => { todo!() }
        FedEventData::SalmonSwim { .. } => { todo!() }
        FedEventData::HitByPitch { .. } => { todo!() }
        FedEventData::SolarPanelsActivate { .. } => { todo!() }
        FedEventData::RunsOverflowing { .. } => { todo!() }
        FedEventData::Middling { .. } => { todo!() }
        FedEventData::EnterCrimeScene { .. } => { todo!() }
        FedEventData::ReturnFromInvestigation { .. } => { todo!() }
        FedEventData::InvestigationConcluded { .. } => { todo!() }
        FedEventData::GrindRail { .. } => { todo!() }
        FedEventData::EnterSecretBase { .. } => { todo!() }
        FedEventData::ExitSecretBase { .. } => { todo!() }
        FedEventData::EchoChamber { .. } => { todo!() }
        FedEventData::Roam { .. } => { todo!() }
    })
}