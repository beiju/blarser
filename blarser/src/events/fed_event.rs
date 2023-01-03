use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use enum_flatten::EnumFlatten;
use fed::{FedEvent as BaseFedEvent, FedEventData, FedEventFlat, FedEventLetsGo, FedEventPlayBall};
use uuid::Uuid;
use partial_information::MaybeKnown;
use crate::entity::{Game, Team};
use crate::events::{AnyEffect, AnyEvent, Effect, EffectVariant, Event};
use crate::events::EarlseasonStart;
use crate::ingest::StateGraph;
use crate::state::EntityType;


#[derive(Debug, Serialize, Deserialize)]
pub struct FedEvent(BaseFedEvent);

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
        // Perhaps one day I will remove the clone requirement here but this is not that day
        let last_update = self.0.clone().last_update();
        // IDE keeps trying to use Iterator::flatten so I'm using UFCS to force it to get the right one
        match EnumFlatten::flatten(self.0) {
            FedEventFlat::BeingSpeech(_) => {
                // BeingSpeech doesn't affect any entities I'm tracking
                Vec::new()
            }
            FedEventFlat::LetsGo(event) => {
                vec![LetsGoEffect::new(event, last_update).into()]
            }
            FedEventFlat::PlayBall(event) => {
                vec![PlayBallGameEffect::new(event, last_update).into()]
            }
            _ => { todo!() }
        }
    }
}

impl Display for FedEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.data.fmt(f)
    }
}

pub fn game_forward(game: &mut Game, game_event: &fed::GameEvent, description: String) {
    game.play_count = game_event.play + 1;

    game.last_update = Some(description);

    game.last_update_full = None;

    // TODO Check the conditionals on this
    game.shame = (game.inning > 8 || game.inning > 7 && !game.top_of_inning) &&
        game.home.score.unwrap() > game.away.score.unwrap();
}

pub fn game_reverse(old_game: &Game, new_game: &mut Game, game_event: &fed::GameEvent) {
    new_game.play_count = game_event.play;  // + 1 for the offset, - 1 for going in reverse

    new_game.last_update = old_game.last_update.clone();

    new_game.last_update_full = old_game.last_update_full.clone();

    new_game.score_update = old_game.score_update.clone();
    new_game.score_ledger = old_game.score_ledger.clone();

    new_game.shame = old_game.shame;
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

#[derive(Clone, Debug)]
pub struct LetsGoEffect {
    event: Arc<FedEventLetsGo>,
    last_update: String,
}

impl LetsGoEffect {
    pub fn new(event: FedEventLetsGo, last_update: String) -> Self {
        Self { event: Arc::new(event), last_update }
    }
}

impl Effect for LetsGoEffect {
    type Variant = LetsGoEffectVariant;

    fn entity_type(&self) -> EntityType { EntityType::Game }

    fn entity_id(&self) -> Option<Uuid> { Some(self.event.game.game_id) }

    fn variant(&self) -> Self::Variant {
        LetsGoEffectVariant::new(self.event.clone(), self.last_update.clone())
    }
}

#[derive(Clone, Debug)]
pub struct LetsGoEffectVariant {
    event: Arc<FedEventLetsGo>,
    // TODO: Try making this a &str (borrowing from LetsGoEffect) and see if it explodes
    last_update: String,
}

impl LetsGoEffectVariant {
    pub fn new(event: Arc<FedEventLetsGo>, description: String) -> Self {
        Self { event, last_update: description }
    }
}

impl EffectVariant for LetsGoEffectVariant {
    type EntityType = Game;

    fn forward(&self, game: &mut Game) {
        game_forward(game, &self.event.game, self.last_update.clone());

        game.game_start = true;
        game.game_start_phase = -1;
        game.home.team_batter_count = Some(-1);
        game.away.team_batter_count = Some(-1);
    }

    fn reverse(&mut self, old_entity: &Self::EntityType, new_entity: &mut Self::EntityType) {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub struct PlayBallGameEffect {
    event: Arc<FedEventPlayBall>,
    last_update: String,
}

impl PlayBallGameEffect {
    pub fn new(event: FedEventPlayBall, last_update: String) -> Self {
        Self { event: Arc::new(event), last_update }
    }
}

impl Effect for PlayBallGameEffect {
    type Variant = PlayBallGameEffectVariant;

    fn entity_type(&self) -> EntityType { EntityType::Game }

    fn entity_id(&self) -> Option<Uuid> { Some(self.event.game.game_id) }

    fn variant(&self) -> Self::Variant {
        PlayBallGameEffectVariant::new(self.event.clone(), self.last_update.clone())
    }
}

#[derive(Clone, Debug)]
pub struct PlayBallGameEffectVariant {
    event: Arc<FedEventPlayBall>,
    // TODO: Try making this a &str (borrowing from PlayBallEffect) and see if it explodes
    last_update: String,
}

impl PlayBallGameEffectVariant {
    pub fn new(event: Arc<FedEventPlayBall>, description: String) -> Self {
        Self { event, last_update: description }
    }
}

impl EffectVariant for PlayBallGameEffectVariant {
    type EntityType = Game;

    fn forward(&self, game: &mut Game) {
        game_forward(game, &self.event.game, self.last_update.clone());

        game.game_start_phase = -1; // not sure about this
        game.inning = -1;
        game.phase = 2;
        game.top_of_inning = false;

        // It unsets pitchers :(
        game.home.pitcher = None;
        game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
        game.home.pitcher_mod = MaybeKnown::Known(String::new());
        game.away.pitcher = None;
        game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));
        game.away.pitcher_mod = MaybeKnown::Known(String::new());
    }

    fn reverse(&mut self, old_game: &Game, new_game: &mut Game) {
        new_game.home.pitcher = old_game.home.pitcher;
        new_game.home.pitcher_name = old_game.home.pitcher_name.clone();
        new_game.home.pitcher_mod = old_game.home.pitcher_mod.clone();
        new_game.away.pitcher = old_game.away.pitcher;
        new_game.away.pitcher_name = old_game.away.pitcher_name.clone();
        new_game.away.pitcher_mod = old_game.away.pitcher_mod.clone();

        // TODO Hard-code these for better error detection
        new_game.game_start_phase = old_game.game_start_phase;
        new_game.inning = old_game.inning;
        new_game.phase = old_game.phase;
        new_game.top_of_inning = old_game.top_of_inning;

        game_reverse(old_game, new_game, &self.event.game);
    }
}

#[derive(Clone, Debug)]
pub struct PlayBallTeamEffect {
    team_id: Uuid,
}

impl PlayBallTeamEffect {
    pub fn new(team_id: Uuid) -> Self { Self { team_id } }
}

impl Effect for PlayBallTeamEffect {
    type Variant = PlayBallTeamEffectVariant;

    fn entity_type(&self) -> EntityType { EntityType::Team }

    fn entity_id(&self) -> Option<Uuid> { Some(self.team_id) }

    fn variant(&self) -> Self::Variant {
        PlayBallTeamEffectVariant::new()
    }
}

#[derive(Clone, Debug)]
pub struct PlayBallTeamEffectVariant;

impl PlayBallTeamEffectVariant {
    pub fn new() -> Self { Self }
}

impl EffectVariant for PlayBallTeamEffectVariant {
    type EntityType = Team;

    fn forward(&self, team: &mut Team) {
        team.rotation_slot += 1;
    }

    fn reverse(&mut self, _: &Team, new_team: &mut Team) {
        new_team.rotation_slot -= 1;
    }
}