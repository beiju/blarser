pub trait PartialInformationCompare {
    fn get_conflicts(&self, other: &Self) -> Vec<String>;
}