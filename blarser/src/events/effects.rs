use std::fmt::Debug;
use as_any::AsAny;
use derive_more::{From, TryInto};
use uuid::Uuid;
use partial_information::MaybeKnown;
use partial_information_derive::PartialInformationCompare;
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
}

impl HitExtrapolated {
    pub fn new(runner: GamePlayerExtrapolated, num_occupied_bases: usize) -> Self {
        Self {
            runner,
            advancements: AdvancementExtrapolated::new(num_occupied_bases),
        }
    }
}

impl Extrapolated for HitExtrapolated {}

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
