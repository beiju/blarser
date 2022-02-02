use serde::{Deserialize, Deserializer};

pub enum Ranged<UnderlyingType> {
    Unknown,
    Known(UnderlyingType),
    Range(UnderlyingType, UnderlyingType),
}

impl<'de, UnderlyingType> Deserialize<'de> for Ranged<UnderlyingType>
    where UnderlyingType: for<'de2> Deserialize<'de2> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de> {
        let val: UnderlyingType = Deserialize::deserialize(deserializer)?;
        Ok(Ranged::Known(val))
    }
}

trait PartialInformationFieldCompare {
    fn get_conflicts(&self, other: &Self) -> Option<String>;
}
