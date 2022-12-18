use std::fmt::Debug;
use std::sync::Arc;
use as_any::{AsAny, Downcast};
use derive_more::{From, TryInto};
use uuid::Uuid;
use partial_information::{MaybeKnown, PartialInformationCompare};
use partial_information_derive::PartialInformationCompare;
use crate::state::EntityType;

pub trait Extrapolated: Debug + AsAny {}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct NullExtrapolated {}

impl Extrapolated for NullExtrapolated {}

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

#[derive(From, TryInto, Clone, Debug)]
#[try_into(owned, ref, ref_mut)]
pub enum AnyExtrapolated {
    Null(NullExtrapolated),
    BatterId(BatterIdExtrapolated),
}
#[derive(From, TryInto, Clone, Debug)]
#[try_into(owned, ref, ref_mut)]
pub enum AnyExtrapolatedRaw {
    NullRaw(<NullExtrapolated as PartialInformationCompare>::Raw),
    BatterIdRaw(<BatterIdExtrapolated as PartialInformationCompare>::Raw),
}

#[derive(Debug)]
pub struct Effect {
    pub ty: EntityType,
    pub id: Option<Uuid>,
    pub extrapolated: AnyExtrapolated,
}

impl Effect {
    pub fn one_id(ty: EntityType, id: Uuid) -> Self {
        Self::one_id_with(ty, id, NullExtrapolated {})
    }

    pub fn all_ids(ty: EntityType) -> Self {
        Self::all_ids_with(ty, NullExtrapolated {})
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
