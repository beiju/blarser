use crate::sim::Entity;

pub struct MergedSuccessors<EntityT: Entity>(Vec<(EntityT, Vec<i32>)>);

impl<EntityT: Entity> MergedSuccessors<EntityT> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add_successors(&mut self, parent: i32, new_successors: Vec<EntityT>) {
        for new_successor in new_successors {
            let found = self.0.iter_mut()
                .find(|(old_successor, _)| new_successor == *old_successor);

            match found {
                Some((_, parents)) => { parents.push(parent) }
                None => { self.0.push((new_successor, vec![parent])) }
            }
        }
    }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn into_inner(self) -> Vec<(EntityT, Vec<i32>)> { self.0 }
}