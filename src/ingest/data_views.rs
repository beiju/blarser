use std::sync::Arc;
use uuid::Uuid;
use crate::blaseball_state::{BlaseballData, PathError, json_path, PrimitiveValue, Event, PathComponent, Path, Node};
use crate::ingest::IngestResult;

pub struct DataView<'data> {
    data: &'data mut BlaseballData,
    caused_by: &'data Arc<Event>,
}

impl<'parent> DataView<'parent> {
    pub fn new<'d>(data: &'d mut BlaseballData, caused_by: &'d Arc<Event>) -> DataView<'d> {
        DataView { data, caused_by }
    }

    pub fn get_team<'short, 'long: 'short>(&'long mut self, team_id: &'short Uuid) -> EntityView<'parent, 'short> {
        EntityView { data_view: self, entity_type: "team", entity_id: team_id }
    }

    pub fn get_game<'short, 'long: 'short>(&'long mut self, game_id: &'short Uuid) -> EntityView<'parent, 'short> {
        EntityView { data_view: self, entity_type: "game", entity_id: game_id }
    }
}

pub struct EntityView<'d, 'e> {
    data_view: &'e mut DataView<'d>,
    entity_type: &'static str,
    entity_id: &'e Uuid,
}

impl<'d, 'e> EntityView<'d, 'e> {
    pub fn get<'new, T: Into<PathComponent>>(&'new mut self, component: T) -> NodeView<'d, 'new> {
        NodeView {
            data_view: self.data_view,
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            path: PathStack::Some((component.into(), &PathStack::None)),
        }
    }

    pub fn set<'set: 'e, T: Into<PrimitiveValue>>(&'set mut self, value: T) -> IngestResult<()> {
        self.data_view.data.get_mut(self.entity_type)
            .ok_or_else(|| PathError::EntityTypeDoesNotExist(self.entity_type))?
            .get_mut(&self.entity_id)
            .ok_or_else(|| PathError::EntityDoesNotExist(self.entity_type, self.entity_id.clone()))?
            .as_primitive_mut()
            .map_err(|value| PathError::UnexpectedType { path: json_path!(self.entity_type, self.entity_id.clone()), expected_type: "object", value })?
            .set(value, self.data_view.caused_by.clone());

        Ok(())
    }
}

enum PathStack<'e> {
    Some((PathComponent, &'e PathStack<'e>)),
    None,
}

pub struct NodeView<'d, 'e> {
    data_view: &'e mut DataView<'d>,
    entity_type: &'static str,
    entity_id: &'e Uuid,
    path: PathStack<'e>,
}

impl<'d, 'e> NodeView<'d, 'e> {
    pub fn get<'new, T: Into<PathComponent>>(&'new mut self, component: T) -> NodeView<'d, 'new> {
        NodeView {
            data_view: self.data_view,
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            path: PathStack::Some((component.into(), &self.path)),
        }
    }

    pub fn set<T: Into<PrimitiveValue>>(&mut self, value: T) -> IngestResult<()> {
        let node = get_node(self.data_view.data, self.entity_type, self.entity_id, &self.path)?;
        *node = node.successor(value.into(), self.data_view.caused_by.clone());

        Ok(())
    }
}


fn get_node<'d>(data: &'d mut BlaseballData, entity_type: &'static str, entity_id: &Uuid, path: &PathStack) -> IngestResult<&'d mut Node> {
    let mut node = data.get_mut(entity_type)
        .ok_or_else(|| PathError::EntityTypeDoesNotExist(entity_type))?
        .get_mut(&entity_id)
        .ok_or_else(|| PathError::EntityDoesNotExist(entity_type, entity_id.clone()))?;

    let mut path_so_far: Vec<&PathComponent> = vec![];
    let mut path_stack = &path;

    let make_path = |path_so_far: &Vec<&PathComponent>| {
        Path {
            entity_type: entity_type,
            entity_id: Some(entity_id.clone()),
            components: path_so_far.iter().cloned().cloned().collect()
        }
    };

    while let PathStack::Some((component, rest)) = path_stack {
        path_stack = rest;
        path_so_far.push(component);

        match component {
            PathComponent::Key(key) => {
                node = node.as_object_mut()
                    .map_err(|value| PathError::UnexpectedType {
                        path: make_path(&path_so_far),
                        expected_type: "object",
                        value,
                    })?
                    .get_mut(key)
                    .ok_or_else(|| PathError::MissingKey(make_path(&path_so_far)))?;
            }
            PathComponent::Index(idx) => {
                node = node.as_array_mut()
                    .map_err(|value| PathError::UnexpectedType {
                        path: make_path(&path_so_far),
                        expected_type: "object",
                        value,
                    })?
                    .get_mut(*idx)
                    .ok_or_else(|| PathError::MissingKey(make_path(&path_so_far)))?;
            }
        }
    }

    Ok(node)
}
