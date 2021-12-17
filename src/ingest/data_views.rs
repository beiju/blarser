use std::fmt::Display;
use std::sync::{Arc, Mutex};
use owning_ref::{MutexGuardRef, MutexGuardRefMut};
use uuid::Uuid;
use serde_json::Value as JsonValue;

use crate::blaseball_state::{BlaseballData, PathError, PrimitiveValue, Event, PathComponent, Path, Node, SharedPrimitiveNode};
use crate::ingest::{IngestResult};

pub enum PathComponentRef<'a> {
    Key(&'a str),
    Index(usize),
}

impl PathComponentRef<'_> {
    fn to_owned(&self) -> PathComponent {
        match self {
            PathComponentRef::Key(key) => { PathComponent::Key(key.to_string()) }
            PathComponentRef::Index(index) => { PathComponent::Index(*index) }
        }
    }
}

impl<'a> From<&'a str> for PathComponentRef<'a> {
    fn from(key: &'a str) -> Self {
        PathComponentRef::Key(key)
    }
}

impl<'a> From<&'a String> for PathComponentRef<'a> {
    fn from(key: &'a String) -> Self {
        PathComponentRef::Key(key)
    }
}

impl From<usize> for PathComponentRef<'_> {
    fn from(index: usize) -> Self {
        PathComponentRef::Index(index)
    }
}

pub trait View<'a> {
    fn get_ref(&self) -> Result<MutexGuardRef<BlaseballData, Node>, PathError>;
    fn get_ref_mut(&self) -> Result<MutexGuardRefMut<BlaseballData, Node>, PathError>;
    fn get_path(&self) -> Path;
    fn caused_by(&self) -> Arc<Event>;
}

pub struct DataView {
    data: Mutex<BlaseballData>,
    caused_by: Arc<Event>,
}

impl DataView {
    pub fn new(data: BlaseballData, caused_by: Event) -> DataView {
        DataView {
            data: Mutex::new(data),
            caused_by: Arc::new(caused_by),
        }
    }

    pub fn into_inner(self) -> (BlaseballData, Arc<Event>) {
        (self.data.into_inner().unwrap(), self.caused_by)
    }

    fn get_ref(&self) -> MutexGuardRef<BlaseballData> {
        MutexGuardRef::from(self.data.lock().unwrap())
    }

    fn get_ref_mut(&self) -> MutexGuardRefMut<BlaseballData> {
        MutexGuardRefMut::from(self.data.lock().unwrap())
    }

    pub fn get_team<'short, 'long: 'short>(&'long self, team_id: &'short Uuid) -> EntityView<'short> {
        EntityView { parent: self, entity_type: "team", entity_id: team_id }
    }

    pub fn get_game<'short, 'long: 'short>(&'long self, game_id: &'short Uuid) -> EntityView<'short> {
        EntityView { parent: self, entity_type: "game", entity_id: game_id }
    }
}

pub struct EntityView<'d> {
    parent: &'d DataView,
    entity_type: &'static str,
    entity_id: &'d Uuid,
}

impl<'a> View<'a> for EntityView<'a> {
    fn get_ref(&self) -> Result<MutexGuardRef<BlaseballData, Node>, PathError> {
        self.parent.get_ref()
            .try_map(|data| {
                data.get(self.entity_type)
                    .ok_or_else(|| PathError::EntityTypeDoesNotExist(self.entity_type))?
                    .get(self.entity_id)
                    .ok_or_else(|| PathError::EntityDoesNotExist(self.entity_type, self.entity_id.clone()))
            })
    }

    fn get_ref_mut(&self) -> Result<MutexGuardRefMut<BlaseballData, Node>, PathError> {
        self.parent.get_ref_mut()
            .try_map_mut(|data| {
                data.get_mut(self.entity_type)
                    .ok_or_else(|| PathError::EntityTypeDoesNotExist(self.entity_type))?
                    .get_mut(self.entity_id)
                    .ok_or_else(|| PathError::EntityDoesNotExist(self.entity_type, self.entity_id.clone()))
            })
    }

    fn get_path(&self) -> Path {
        Path {
            entity_type: self.entity_type,
            entity_id: Some(self.entity_id.clone()),
            components: vec![]
        }
    }

    fn caused_by(&self) -> Arc<Event> {
        self.parent.caused_by.clone()
    }
}

impl<'e> EntityView<'e> {
    pub fn get<T: Into<PathComponentRef<'e>>>(&'e self, key: T) -> NodeView<Self> {
        NodeView {
            parent: self,
            key: key.into(),
        }
    }
}

pub struct NodeView<'view, ParentT: View<'view>> {
    parent: &'view ParentT,
    key: PathComponentRef<'view>,
}

impl<'view, ParentT: View<'view>> View<'view> for NodeView<'view, ParentT> {
    fn get_ref(&self) -> Result<MutexGuardRef<BlaseballData, Node>, PathError> {
        self.parent.get_ref()?
            .try_map(|parent| {
                match self.key {
                    PathComponentRef::Key(key) => {
                        parent.as_object()
                            .map_err(|value| PathError::UnexpectedType {
                                path: self.get_path(),
                                expected_type: "object",
                                value
                            })?
                            .get(key)
                            .ok_or_else(|| PathError::MissingKey(self.get_path()))
                    }
                    PathComponentRef::Index(idx) => {
                        parent.as_array()
                            .map_err(|value| PathError::UnexpectedType {
                                path: self.get_path(),
                                expected_type: "object",
                                value
                            })?
                            .get(idx)
                            .ok_or_else(|| PathError::MissingKey(self.get_path()))

                    }
                }
            })
    }

    fn get_ref_mut(&self) -> Result<MutexGuardRefMut<BlaseballData, Node>, PathError> {
        self.parent.get_ref_mut()?
            .try_map_mut(|parent| {
                match self.key {
                    PathComponentRef::Key(key) => {
                        parent.as_object_mut()
                            .map_err(|value| PathError::UnexpectedType {
                                path: self.get_path(),
                                expected_type: "object",
                                value
                            })?
                            .get_mut(key)
                            .ok_or_else(|| PathError::MissingKey(self.get_path()))
                    }
                    PathComponentRef::Index(idx) => {
                        parent.as_array_mut()
                            .map_err(|value| PathError::UnexpectedType {
                                path: self.get_path(),
                                expected_type: "object",
                                value
                            })?
                            .get_mut(idx)
                            .ok_or_else(|| PathError::MissingKey(self.get_path()))

                    }
                }
            })
    }

    fn get_path(&self) -> Path {
        self.parent.get_path().extend(self.key.to_owned())
    }

    fn caused_by(&self) -> Arc<Event> {
        self.parent.caused_by()
    }
}

impl<'view, ParentT: View<'view>> NodeView<'view, ParentT> {
    pub fn get<'a: 'view, T: Into<PathComponentRef<'a>>>(&'a self, key: T) -> NodeView<'a, Self> {
        NodeView {
            parent: self,
            key: key.into(),
        }
    }

    pub async fn as_int(&self) -> IngestResult<i64> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().await;

        let value = lock.value.as_int()
            .ok_or_else(|| self.path_error("int", &lock.value))?;

        Ok(*value)
    }

    pub async fn as_bool(&self) -> IngestResult<bool> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().await;

        let value = lock.value.as_bool()
            .ok_or_else(|| self.path_error("bool", &lock.value))?;

        Ok(*value)
    }

    pub async fn as_uuid(&self) -> IngestResult<Uuid> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().await;

        let value = lock.value.as_uuid()
            .ok_or_else(|| self.path_error("uuid", &lock.value))?;

        Ok(value)
    }

    pub async fn as_string(&self) -> IngestResult<String> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().await;

        let value = lock.value.as_str()
            .ok_or_else(|| self.path_error("string", &lock.value))?;

        Ok(value.to_owned())
    }

    fn as_primitive(&self) -> Result<MutexGuardRef<BlaseballData, SharedPrimitiveNode>, PathError> {
        self.get_ref()?
            .try_map(|node| {
                node.as_primitive()
                    .map_err(|value| PathError::UnexpectedType {
                        path: self.get_path(),
                        expected_type: "int",
                        value,
                    })
            })
    }

    pub fn set<T: Into<PrimitiveValue>>(&mut self, value: T) -> IngestResult<()> {
        let mut node = self.get_ref_mut()?;
        *node = node.successor(value.into(), self.caused_by().clone());

        Ok(())
    }

    pub fn overwrite<T: Into<JsonValue>>(&mut self, value: T) -> IngestResult<()> {
        let mut node = self.get_ref_mut()?;
        *node = Node::new_from_json(value.into(), self.caused_by());

        Ok(())
    }

    fn path_error<ValueT: Display>(&self, expected_type: &'static str, value: ValueT) -> PathError {
        PathError::UnexpectedType {
            path: self.get_path(),
            expected_type,
            value: value.to_string()
        }
    }
}