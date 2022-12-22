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
pub struct SubsecondsExtrapolated {
    pub(crate) ns: MaybeKnown<u32>,
}

impl Extrapolated for SubsecondsExtrapolated {}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct BatterIdExtrapolated {
    pub(crate) batter_id: Option<Uuid>,
}

impl BatterIdExtrapolated {
    pub fn new(batter_id: Option<Uuid>) -> Self {
        Self { batter_id }
    }
}

impl Extrapolated for BatterIdExtrapolated {}

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

#[derive(Default, Debug, Clone, PartialInformationCompare)]
pub struct OddsAndPitchersExtrapolated {
    pub away: PitcherExtrapolated,
    pub home: PitcherExtrapolated,
    pub away_odds: MaybeKnown<f32>,
    pub home_odds: MaybeKnown<f32>,
}

impl Extrapolated for OddsAndPitchersExtrapolated {}

polymorphic_enum! {
    #[derive(From, TryInto, Clone, Debug)]
    #[try_into(owned, ref, ref_mut)]
    pub AnyExtrapolated: with_extrapolated {
        Null(NullExtrapolated),
        Subseconds(SubsecondsExtrapolated),
        BatterId(BatterIdExtrapolated),
        Pitchers(PitchersExtrapolated),
        OddsAndPitchers(OddsAndPitchersExtrapolated),
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
