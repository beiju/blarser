use std::fmt::{Display, Formatter};
use std::sync::Arc;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use enum_flatten::EnumFlatten;
use fed::{FedEvent as BaseFedEvent, FedEventData, FedEventLetsGo, FedEventFlat};
use uuid::Uuid;
use crate::entity::Game;
use crate::events::{AnyEffect, AnyEvent, Effect, EffectVariant, Event, ord_by_time};
use crate::events::EarlseasonStart;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct FedEvent(BaseFedEvent);
ord_by_time!(FedEvent);

impl FedEvent {
    pub fn new(event: BaseFedEvent) -> Self {
        Self(event)
    }
}

impl Event for FedEvent {
    fn time(&self) -> DateTime<Utc> {
        self.0.created
    }

    fn generate_predecessor(&self, state: &StateGraph) -> Option<AnyEvent> {
        match &self.0.data {
            FedEventData::LetsGo { .. } => {
                if state.query_sim_unique(|sim| sim.phase) == 1 {
                    Some(EarlseasonStart::new(self.0.created, self.0.season).into())
                } else {
                    None
                }
            }
            _ => { None }
        }
    }

    fn into_effects(self, _: &StateGraph) -> Vec<AnyEffect> {
        // IDE keeps trying to use Iterator::flatten so I'm using UFCS to force it to get the right one
       match EnumFlatten::flatten(self.0) {
            FedEventFlat::BeingSpeech(_) => {
                // BeingSpeech doesn't affect any entities I'm tracking
                Vec::new()
            }
            FedEventFlat::LetsGo(event) => {
                vec![LetsGoEffect::new(event).into()]
            }
            FedEventFlat::PlayBall(_) => { todo!() }
            FedEventFlat::HalfInningStart(_) => { todo!() }
            FedEventFlat::BatterUp(_) => { todo!() }
            FedEventFlat::SuperyummyGameStart(_) => { todo!() }
            FedEventFlat::EchoedSuperyummyGameStart(_) => { todo!() }
            FedEventFlat::Ball(_) => { todo!() }
            FedEventFlat::FoulBall(_) => { todo!() }
            FedEventFlat::StrikeSwinging(_) => { todo!() }
            FedEventFlat::StrikeLooking(_) => { todo!() }
            FedEventFlat::StrikeFlinching(_) => { todo!() }
            FedEventFlat::Flyout(_) => { todo!() }
            FedEventFlat::GroundOut(_) => { todo!() }
            FedEventFlat::FieldersChoice(_) => { todo!() }
            FedEventFlat::DoublePlay(_) => { todo!() }
            FedEventFlat::Hit(_) => { todo!() }
            FedEventFlat::HomeRun(_) => { todo!() }
            FedEventFlat::StolenBase(_) => { todo!() }
            FedEventFlat::CaughtStealing(_) => { todo!() }
            FedEventFlat::StrikeoutSwinging(_) => { todo!() }
            FedEventFlat::StrikeoutLooking(_) => { todo!() }
            FedEventFlat::Walk(_) => { todo!() }
            FedEventFlat::InningEnd(_) => { todo!() }
            FedEventFlat::CharmStrikeout(_) => { todo!() }
            FedEventFlat::StrikeZapped(_) => { todo!() }
            FedEventFlat::PeanutFlavorText(_) => { todo!() }
            FedEventFlat::GameEnd(_) => { todo!() }
            FedEventFlat::MildPitch(_) => { todo!() }
            FedEventFlat::MildPitchWalk(_) => { todo!() }
            FedEventFlat::CoffeeBean(_) => { todo!() }
            FedEventFlat::BecameMagmatic(_) => { todo!() }
            FedEventFlat::Blooddrain(_) => { todo!() }
            FedEventFlat::SpecialBlooddrain(_) => { todo!() }
            FedEventFlat::PlayerModExpires(_) => { todo!() }
            FedEventFlat::TeamModExpires(_) => { todo!() }
            FedEventFlat::BirdsCircle(_) => { todo!() }
            FedEventFlat::AmbushedByCrows(_) => { todo!() }
            FedEventFlat::Sun2SetWin(_) => { todo!() }
            FedEventFlat::BlackHoleSwallowedWin(_) => { todo!() }
            FedEventFlat::Sun2(_) => { todo!() }
            FedEventFlat::BlackHole(_) => { todo!() }
            FedEventFlat::TeamDidShame(_) => { todo!() }
            FedEventFlat::TeamWasShamed(_) => { todo!() }
            FedEventFlat::CharmWalk(_) => { todo!() }
            FedEventFlat::GainFreeRefill(_) => { todo!() }
            FedEventFlat::AllergicReaction(_) => { todo!() }
            FedEventFlat::PerkUp(_) => { todo!() }
            FedEventFlat::Feedback(_) => { todo!() }
            FedEventFlat::BestowReverberating(_) => { todo!() }
            FedEventFlat::Reverb(_) => { todo!() }
            FedEventFlat::TarotReading(_) => { todo!() }
            FedEventFlat::TarotReadingAddedOrRemovedMod(_) => { todo!() }
            FedEventFlat::TeamEnteredPartyTime(_) => { todo!() }
            FedEventFlat::BecomeTripleThreat(_) => { todo!() }
            FedEventFlat::UnderOver(_) => { todo!() }
            FedEventFlat::OverUnder(_) => { todo!() }
            FedEventFlat::TasteTheInfinite(_) => { todo!() }
            FedEventFlat::BatterSkipped(_) => { todo!() }
            FedEventFlat::FeedbackBlocked(_) => { todo!() }
            FedEventFlat::FlagPlanted(_) => { todo!() }
            FedEventFlat::EmergencyAlert(_) => { todo!() }
            FedEventFlat::TeamJoinedILB(_) => { todo!() }
            FedEventFlat::FloodingSwept(_) => { todo!() }
            FedEventFlat::ReturnFromElsewhere(_) => { todo!() }
            FedEventFlat::Incineration(_) => { todo!() }
            FedEventFlat::PitcherChange(_) => { todo!() }
            FedEventFlat::Party(_) => { todo!() }
            FedEventFlat::PlayerHatched(_) => { todo!() }
            FedEventFlat::PostseasonBirth(_) => { todo!() }
            FedEventFlat::FinalStandings(_) => { todo!() }
            FedEventFlat::TeamLeftPartyTimeForPostseason(_) => { todo!() }
            FedEventFlat::EarnedPostseasonSlot(_) => { todo!() }
            FedEventFlat::PostseasonAdvance(_) => { todo!() }
            FedEventFlat::PostseasonEliminated(_) => { todo!() }
            FedEventFlat::PlayerBoosted(_) => { todo!() }
            FedEventFlat::TeamWonInternetSeries(_) => { todo!() }
            FedEventFlat::BottomDwellers(_) => { todo!() }
            FedEventFlat::WillReceived(_) => { todo!() }
            FedEventFlat::BlessingWon(_) => { todo!() }
            FedEventFlat::EarlbirdsAdded(_) => { todo!() }
            FedEventFlat::DecreePassed(_) => { todo!() }
            FedEventFlat::PlayerJoinedILB(_) => { todo!() }
            FedEventFlat::PlayerPermittedToStay(_) => { todo!() }
            FedEventFlat::FireproofIncineration(_) => { todo!() }
            FedEventFlat::LineupSorted(_) => { todo!() }
            FedEventFlat::EarlbirdsRemoved(_) => { todo!() }
            FedEventFlat::Undersea(_) => { todo!() }
            FedEventFlat::RenovationBuilt(_) => { todo!() }
            FedEventFlat::LateToThePartyAdded(_) => { todo!() }
            FedEventFlat::PeanutMister(_) => { todo!() }
            FedEventFlat::PlayerNamedMvp(_) => { todo!() }
            FedEventFlat::LateToThePartyRemoved(_) => { todo!() }
            FedEventFlat::BirdsUnshell(_) => { todo!() }
            FedEventFlat::ReplaceReturnedPlayerFromShadows(_) => { todo!() }
            FedEventFlat::PlayerCalledBackToHall(_) => { todo!() }
            FedEventFlat::TeamUsedFreeWill(_) => { todo!() }
            FedEventFlat::PlayerLostMod(_) => { todo!() }
            FedEventFlat::InvestigationMessage(_) => { todo!() }
            FedEventFlat::HighPressure(_) => { todo!() }
            FedEventFlat::PlayerPulledThroughRift(_) => { todo!() }
            FedEventFlat::PlayerLocalized(_) => { todo!() }
            FedEventFlat::Echo(_) => { todo!() }
            FedEventFlat::SolarPanelsAwait(_) => { todo!() }
            FedEventFlat::EchoIntoStatic(_) => { todo!() }
            FedEventFlat::Psychoacoustics(_) => { todo!() }
            FedEventFlat::EchoReceiver(_) => { todo!() }
            FedEventFlat::ConsumerAttack(_) => { todo!() }
            FedEventFlat::TeamGainedFreeWill(_) => { todo!() }
            FedEventFlat::Tidings(_) => { todo!() }
            FedEventFlat::HomebodyGameStart(_) => { todo!() }
            FedEventFlat::SalmonSwim(_) => { todo!() }
            FedEventFlat::HitByPitch(_) => { todo!() }
            FedEventFlat::SolarPanelsActivate(_) => { todo!() }
            FedEventFlat::RunsOverflowing(_) => { todo!() }
            FedEventFlat::Middling(_) => { todo!() }
            FedEventFlat::EnterCrimeScene(_) => { todo!() }
            FedEventFlat::ReturnFromInvestigation(_) => { todo!() }
            FedEventFlat::InvestigationConcluded(_) => { todo!() }
            FedEventFlat::GrindRail(_) => { todo!() }
            FedEventFlat::EnterSecretBase(_) => { todo!() }
            FedEventFlat::ExitSecretBase(_) => { todo!() }
            FedEventFlat::EchoChamber(_) => { todo!() }
            FedEventFlat::Roam(_) => { todo!() }
            FedEventFlat::GlitterCrate(_) => { todo!() }
            FedEventFlat::ModsFromAnotherModRemoved(_) => { todo!() }
        }
    }
}

impl Display for FedEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.data.fmt(f)
    }
}

pub fn game_score_forward(game: &mut Game, scoring_players: &[fed::ScoringPlayer], free_refills: &[fed::FreeRefill]) {
    let mut runs_scored = 0.;
    for score in scoring_players {
        game.pop_base_runner(score.player_id);
        runs_scored += 1.;
    }
    game.score_update = Some(format!("{runs_scored} Run{} scored!",
                                     if runs_scored != 1. { "s" } else { "" }));
    game.half_inning_score += runs_scored;
    *game.team_at_bat_mut().score.as_mut().unwrap() += runs_scored;
    *game.current_half_score_mut() += runs_scored;
    // There cant be free refills without scores [falsehoods] so it's fine to do this here
    game.half_inning_outs -= free_refills.len() as i32;
}

pub fn game_score_reverse(old_game: &Game, new_game: &mut Game, scoring_players: &[fed::ScoringPlayer], free_refills: &[fed::FreeRefill]) {
    // I think re-using the iterator will let us properly handle multiple of the same
    // player. Using enumerate to get index rather than find_position because I think
    // find_position will reset the index.
    //
    // This is made much more complicated by just a few games where players could score
    // from positions other than the front of the array.
    let mut old_base_runners_it = old_game.base_runners.iter()
        .enumerate();
    for scorer in scoring_players {
        let (idx, _) = old_base_runners_it
            .find(|(_, &id)| id == scorer.player_id)
            .expect("The scorer must be present in the base_runners list");
        new_game.base_runners.insert(idx, old_game.base_runners[idx].clone());
        new_game.base_runner_names.insert(idx, old_game.base_runner_names[idx].clone());
        new_game.base_runner_mods.insert(idx, old_game.base_runner_mods[idx].clone());
        new_game.bases_occupied.insert(idx, old_game.bases_occupied[idx].clone());
        new_game.baserunner_count += 1;
    }
    new_game.half_inning_score = old_game.half_inning_score;
    new_game.team_at_bat_mut().score = old_game.team_at_bat().score;
    *new_game.current_half_score_mut() = old_game.current_half_score();
    // There cant be free refills without scores [falsehoods] so it's fine to do this here
    new_game.half_inning_outs += free_refills.len() as i32;
}


impl EffectVariant for fed::GameEvent {
    type EntityType = Game;

    fn forward(&self, game: &mut Game) {
        game.play_count = self.play + 1;

        game.last_update = Some(self.description.clone());

        game.last_update_full = None;

        if let Some(score) = &self.scores && !score.scores.is_empty() {
            game_score_forward(game, &score.scores, &score.free_refills);
        } else {
            game.score_update = Some(String::new());
        }

        // TODO Check the conditionals on this
        game.shame = (game.inning > 8 || game.inning > 7 && !game.top_of_inning) &&
            game.home.score.unwrap() > game.away.score.unwrap();
    }

    fn reverse(&mut self, old_game: &Game, new_game: &mut Game) {
        new_game.play_count = self.play;

        new_game.last_update = old_game.last_update.clone();

        new_game.last_update_full = old_game.last_update_full.clone();

        new_game.score_update = old_game.score_update.clone();
        new_game.score_ledger = old_game.score_ledger.clone();

        new_game.score_update = old_game.score_update.clone();
        if let Some(score) = &self.scores && !score.scores.is_empty() {
            game_score_reverse(old_game, new_game, &score.scores, &score.free_refills);
        }

        new_game.shame = old_game.shame;
    }
}

#[derive(Clone, Debug)]
pub struct LetsGoEffect {
    event: Arc<FedEventLetsGo>
}

impl LetsGoEffect {
    pub fn new(event: FedEventLetsGo) -> Self { Self { event: Arc::new(event) } }
}

impl Effect for LetsGoEffect {
    type Variant = LetsGoEffectVariant;

    fn entity_type(&self) -> EntityType { EntityType::Game }

    fn entity_id(&self) -> Option<Uuid> { Some(self.event.game.game_id) }

    fn variant(&self) -> Self::Variant {
        LetsGoEffectVariant::new(self.event.clone())
    }
}

#[derive(Clone, Debug)]
pub struct LetsGoEffectVariant {
    event: Arc<FedEventLetsGo>
}

impl LetsGoEffectVariant {
    pub fn new(event: Arc<FedEventLetsGo>) -> Self { Self { event } }
}

impl EffectVariant for LetsGoEffectVariant {
    type EntityType = Game;

    fn forward(&self, game: &mut Game) {
        self.event.game.forward(game);

        game.game_start = true;
        game.game_start_phase = -1;
        game.home.team_batter_count = Some(-1);
        game.away.team_batter_count = Some(-1);
    }

    fn reverse(&mut self, old_entity: &Self::EntityType, new_entity: &mut Self::EntityType) {
        todo!()
    }
}