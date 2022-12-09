use std::fmt::Debug;
use std::sync::Arc;
use as_any::{AsAny, Downcast};
use uuid::Uuid;
use partial_information::PartialInformationCompare;
use partial_information_derive::PartialInformationCompare;
use crate::state::EntityType;

pub trait Extrapolated: Debug + AsAny {}

pub type AnyExtrapolated = Arc<dyn Extrapolated + Send + Sync>;

#[derive(Debug, PartialInformationCompare)]
struct NullExtrapolated {}

impl Extrapolated for NullExtrapolated {}


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

    pub fn one_id_with<T: Extrapolated + Send + Sync>(ty: EntityType, id: Uuid, extrapolated: T) -> Self {
        Self { ty, id: Some(id), extrapolated: Arc::new(extrapolated) }
    }

    pub fn all_ids_with<T: Extrapolated + Send + Sync>(ty: EntityType, extrapolated: T) -> Self {
        Self { ty, id: None, extrapolated: Arc::new(extrapolated) }
    }

    pub fn null_id_with<T: Extrapolated + Send + Sync>(ty: EntityType, extrapolated: T) -> Self {
        Self::one_id_with(ty, Uuid::nil(), extrapolated)
    }
}
