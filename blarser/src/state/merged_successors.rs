#[derive(Clone)]
pub struct MergedSuccessors<T: PartialEq>(Vec<(T, Vec<i32>)>);

impl<T: PartialEq> MergedSuccessors<T> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add_successors(&mut self, parent: i32, new_successors: impl IntoIterator<Item=T>) {
        for new_successor in new_successors {
            self.add_successor(parent, new_successor)
        }
    }

    pub fn add_successor(&mut self, parent: i32, new_successor: T) {
        let found = self.0.iter_mut()
            .find(|(old_successor, _)| new_successor == *old_successor);

        match found {
            Some((_, parents)) => {
                assert!(!parents.contains(&parent), "Tried to add multiple identical successors");
                parents.push(parent)
            }
            None => { self.0.push((new_successor, vec![parent])) }
        }
    }

    pub fn add_multi_parent_successor(&mut self, parents: Vec<i32>, new_successor: T) {
        let found = self.0.iter_mut()
            .find(|(old_successor, _)| new_successor == *old_successor);

        match found {
            Some((_, existing_parents)) => { existing_parents.extend(parents.iter()) }
            None => { self.0.push((new_successor, parents)) }
        }
    }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn inner(&self) -> &Vec<(T, Vec<i32>)> { &self.0 }
    pub fn into_inner(self) -> Vec<(T, Vec<i32>)> { self.0 }

    pub fn iter(&self) -> impl Iterator<Item=&(T, Vec<i32>)> { self.0.iter() }
}