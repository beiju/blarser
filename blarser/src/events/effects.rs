use std::fmt::Debug;
use as_any::AsAny;
use derive_more::{From, TryInto};
use uuid::Uuid;
use partial_information::MaybeKnown;
use partial_information_derive::PartialInformationCompare;
use crate::entity::AnyEntity;
use crate::polymorphic_enum::polymorphic_enum;
use crate::state::EntityType;

pub trait Extrapolated: Debug + AsAny {


    fn observe_entity(&self, entity: &AnyEntity) -> Self;
}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct NullExtrapolated {}

impl Extrapolated for NullExtrapolated {
    fn observe_entity(&self, _: &AnyEntity) -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct SubsecondsExtrapolated {
    pub(crate) ns: MaybeKnown<u32>,
}

impl Extrapolated for SubsecondsExtrapolated {
    fn observe_entity(&self, entity: &AnyEntity) -> Self {
        let sim = entity.as_sim()
            .expect("TODO: Strongly type this?");
        Self {
            ns: sim.next_phase_time.ns()
        }
    }
}

#[derive(Debug, Clone, PartialInformationCompare)]
pub struct BatterIdExtrapolated {
    pub(crate) batter_id: Option<Uuid>,
}

impl BatterIdExtrapolated {
    pub fn new(batter_id: Option<Uuid>) -> Self {
        Self { batter_id }
    }
}

impl Extrapolated for BatterIdExtrapolated {
    fn observe_entity(&self, _: &AnyEntity) -> Self {
        self.clone()
    }
}

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

impl Extrapolated for PitchersExtrapolated {
    fn observe_entity(&self, entity: &AnyEntity) -> Self {
        let game = entity.as_game()
            .expect("TODO: Strongly type this?");
        Self {
            away: PitcherExtrapolated {
                pitcher_id: game.away.pitcher.clone()
                    .expect("There should be an away pitcher at this point"),
                pitcher_name: game.away.pitcher_name.clone()
                    .expect("There should be an away pitcher name at this point"),
                pitcher_mod: game.away.pitcher_mod.clone(),
            },
            home: PitcherExtrapolated {
                pitcher_id: game.home.pitcher.clone()
                    .expect("There should be a home pitcher at this point"),
                pitcher_name: game.home.pitcher_name.clone()
                    .expect("There should be a home pitcher name at this point"),
                pitcher_mod: game.home.pitcher_mod.clone(),
            },
        }
    }
}
#[derive(Debug, Clone, PartialInformationCompare)]
pub struct OddsExtrapolated {
    pub away_odds: MaybeKnown<f32>,
    pub home_odds: MaybeKnown<f32>,
}

impl Extrapolated for OddsExtrapolated {
    fn observe_entity(&self, entity: &AnyEntity) -> Self {
        let game = entity.as_game()
            .expect("TODO: Strongly type this?");
        Self {
            away_odds: game.away.odds.clone()
                .expect("There should be game odds at this point"),
            home_odds: game.home.odds.clone()
                .expect("There should be game odds at this point"),
        }
    }
}

// #[derive(From, TryInto, Clone, Debug)]
// #[try_into(owned, ref, ref_mut)]
// pub enum AnyExtrapolated {
//     Null(NullExtrapolated),
//     BatterId(BatterIdExtrapolated),
// }

polymorphic_enum! {
    #[derive(From, TryInto, Clone, Debug)]
    #[try_into(owned, ref, ref_mut)]
    pub AnyExtrapolated: with_extrapolated {
        Null(NullExtrapolated),
        Subseconds(SubsecondsExtrapolated),
        BatterId(BatterIdExtrapolated),
        Pitchers(PitchersExtrapolated),
        Odds(OddsExtrapolated),
    }
}

impl AnyExtrapolated {
    pub(crate) fn observe_entity(&self, entity: &AnyEntity) -> Self {
        with_extrapolated!(self, |e| { e.observe_entity(entity).into() })
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
