use crate::StateInterface;

pub trait IngestEvent {
    fn apply(&self, state: &mut StateInterface);
}