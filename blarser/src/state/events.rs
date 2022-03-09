use crate::StateInterface;

pub trait IngestEvent {
    fn apply(&self, state: &impl StateInterface);
}