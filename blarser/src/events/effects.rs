use std::collections::HashMap;
use std::fmt::Debug;
use std::iter;
use as_any::AsAny;
use derive_more::{From, TryInto};
use fed::{FreeRefill, ScoreInfo};
use itertools::zip_eq;
use uuid::Uuid;
use partial_information::MaybeKnown;
use partial_information_derive::PartialInformationCompare;
use crate::entity::Game;
use crate::events::event_util::{get_displayed_mod_excluding, PITCHER_MOD_PRECEDENCE, RUNNER_MOD_PRECEDENCE};
use crate::ingest::StateGraph;
use crate::polymorphic_enum::polymorphic_enum;
use crate::state::EntityType;

pub trait Extrapolated: Debug + AsAny {}

#[derive(Default, Debug, Clone, PartialInformationCompare)]
pub struct NullExtrapolated {}

impl Extrapolated for NullExtrapolated {}

#[derive(Default, Debug, Clone, PartialInformationCompare)]
pub struct EarlseasonStartSubsecondsExtrapolated {
    pub(crate) gods_day_ns: MaybeKnown<u32>,
    pub(crate) next_phase_ns: MaybeKnown<u32>,
}

impl Extrapolated for EarlseasonStartSubsecondsExtrapolated {}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct GamePlayerExtrapolated {
    pub(crate) player_id: Uuid,
    pub(crate) player_mod: String,
}

impl GamePlayerExtrapolated {
    pub fn new(player_id: Uuid, player_mod: String) -> Self {
        Self { player_id, player_mod }
    }
}

impl Extrapolated for GamePlayerExtrapolated {}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct HitExtrapolated {
    pub(crate) runner: GamePlayerExtrapolated,
    pub(crate) advancements: AdvancementExtrapolated,
    pub(crate) mod_changes: DisplayedModChangeExtrapolated,
}

impl Extrapolated for HitExtrapolated {}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct DisplayedModChangeExtrapolated {
    pub(crate) new_pitcher_mod: Option<String>,
    pub(crate) new_runner_mods: HashMap<Uuid, Option<String>>,
}

impl DisplayedModChangeExtrapolated {
    pub fn new(game_id: Uuid, refills: &[FreeRefill], state: &StateGraph) -> Self {
        let pitcher_id = state.query_game_unique(game_id, |game| {
            *game.defending_team().pitcher
                .expect("There must be a pitcher during a Free-Refill-eligible event")
                .known()
                .expect("Pitcher must be known during a Free-Refill-eligible event")
        });

        let batter_id = state.query_game_unique(game_id, |game| {
            game.team_at_bat().batter
                .expect("There must be a batter during a Free-Refill-eligible event")
        });

        let runner_ids = state.query_game_unique(game_id, |game| game.base_runners.clone());

        fn displayed_mod(state: &StateGraph, refills: &[FreeRefill], player_id: Uuid, mods_to_display: &[&str]) -> Option<String> {
            if refills.iter().any(|refill| refill.player_id == player_id) {
                Some(get_displayed_mod_excluding(state, player_id, &["COFFEE_RALLY"], mods_to_display))
            } else {
                None
            }
        }

        let new_pitcher_mod = displayed_mod(state, refills, pitcher_id, &PITCHER_MOD_PRECEDENCE);

        let new_runner_mods = runner_ids.iter()
            .chain(iter::once(&batter_id))
            .map(|&runner_id| {
                (runner_id, displayed_mod(state, refills, runner_id, &RUNNER_MOD_PRECEDENCE))
            })
            .collect();

        Self {
            new_pitcher_mod,
            new_runner_mods,
        }
    }
    
    pub fn forward(&self, game: &mut Game) {
        if let Some(new_mod) = &self.new_pitcher_mod {
            game.defending_team_mut().pitcher_mod = MaybeKnown::Known(new_mod.clone());
        }

        for (runner_id, runner_mod) in zip_eq(&game.base_runners, &mut game.base_runner_mods) {
            let new_mod = self.new_runner_mods.get(runner_id)
                .expect("Extrapolated should have an entry for every runner");
            if let Some(new_mod) = new_mod {
                *runner_mod = new_mod.clone();
            }
        }
    }
    
    pub fn reverse(&self, old_game: &Game, new_game: &mut Game) {
        if self.new_pitcher_mod.is_some() {
            new_game.defending_team_mut().pitcher_mod = old_game.defending_team().pitcher_mod.clone();
        }

        for (runner_id, (old_mod, new_mod)) in zip_eq(&old_game.base_runners, zip_eq(&old_game.base_runner_mods, &mut new_game.base_runner_mods)) {
            let extrapolated_mod = self.new_runner_mods.get(runner_id)
                .expect("Extrapolated should have an entry for every runner");
            if extrapolated_mod.is_some() {
                *new_mod = old_mod.clone();
            }
        }
    }
}

impl Extrapolated for DisplayedModChangeExtrapolated {}

#[derive(Default, Debug, Clone, PartialInformationCompare)]
pub struct PitcherExtrapolated {
    pub pitcher_id: MaybeKnown<Uuid>,
    pub pitcher_name: MaybeKnown<String>,
    pub pitcher_mod: MaybeKnown<String>,
}

#[derive(Default, Debug, Clone, PartialInformationCompare)]
pub struct PitchersExtrapolated {
    pub away: PitcherExtrapolated,
    pub home: PitcherExtrapolated,
}

impl PitchersExtrapolated {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Extrapolated for PitchersExtrapolated {}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct OddsAndPitchersExtrapolated {
    pub away: PitcherExtrapolated,
    pub home: PitcherExtrapolated,
    pub away_odds: MaybeKnown<f32>,
    pub home_odds: MaybeKnown<f32>,
}

impl Extrapolated for OddsAndPitchersExtrapolated {}

impl Default for OddsAndPitchersExtrapolated {
    fn default() -> Self {
        Self {
            away: Default::default(),
            home: Default::default(),
            away_odds: MaybeKnown::UnknownExcluding(0.),
            home_odds: MaybeKnown::UnknownExcluding(0.),
        }
    }
}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct AdvancementExtrapolated {
    // This is a vec parallel to `baserunners`, `basesOccupied`, etc. Each element a MaybeUnknown
    // bool representing whether that player advanced (or, for hit events, whether they advanced an
    // extra base)
    pub bases: Vec<MaybeKnown<bool>>,
}

impl Extrapolated for AdvancementExtrapolated {}

impl AdvancementExtrapolated {
    pub fn new(num_occupied_bases: usize) -> Self {
        Self {
            bases: vec![MaybeKnown::Unknown; num_occupied_bases],
        }
    }
}

polymorphic_enum! {
    #[derive(From, TryInto, Clone, Debug)]
    #[try_into(owned, ref, ref_mut)]
    pub AnyExtrapolated: with_extrapolated {
        Null(NullExtrapolated),
        Subseconds(EarlseasonStartSubsecondsExtrapolated),
        GamePlayer(GamePlayerExtrapolated),
        Pitchers(PitchersExtrapolated),
        OddsAndPitchers(OddsAndPitchersExtrapolated),
        Advancement(AdvancementExtrapolated),
        Hit(HitExtrapolated),
        DisplayedModChange(DisplayedModChangeExtrapolated),
    }
}

#[derive(Debug)]
pub struct Effect {
    pub ty: EntityType,
    pub id: Option<Uuid>,
    pub extrapolated: AnyExtrapolated,
}

impl Effect {
    pub fn one_id(ty: EntityType, id: Uuid) -> Self {
        Self::one_id_with(ty, id, NullExtrapolated::default())
    }

    pub fn all_ids(ty: EntityType) -> Self {
        Self::all_ids_with(ty, NullExtrapolated::default())
    }

    pub fn null_id(ty: EntityType) -> Self {
        Self::one_id(ty, Uuid::nil())
    }

    pub fn one_id_with<T>(ty: EntityType, id: Uuid, extrapolated: T) -> Self
        where T: Extrapolated + Send + Sync, AnyExtrapolated: From<T> {
        Self { ty, id: Some(id), extrapolated: AnyExtrapolated::from(extrapolated) }
    }

    pub fn all_ids_with<T>(ty: EntityType, extrapolated: T) -> Self
        where T: Extrapolated + Send + Sync, AnyExtrapolated: From<T> {
        Self { ty, id: None, extrapolated: AnyExtrapolated::from(extrapolated) }
    }

    pub fn null_id_with<T>(ty: EntityType, extrapolated: T) -> Self
        where T: Extrapolated + Send + Sync, AnyExtrapolated: From<T> {
        Self::one_id_with(ty, Uuid::nil(), extrapolated)
    }
}
