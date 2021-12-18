use std::fmt::Display;
use std::sync::{Arc, Mutex};
use owning_ref::{MutexGuardRef, MutexGuardRefMut};
use uuid::Uuid;
use serde_json::Value as JsonValue;

use crate::blaseball_state::{BlaseballData, PathError, PrimitiveValue, Event, PathComponent, Path, Node, SharedPrimitiveNode};

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

const UUID_NIL: Uuid = Uuid::nil();

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

    pub fn get_sim<'short, 'long: 'short>(&'long self) -> EntityView<'short> {
        EntityView { parent: self, entity_type: "sim", entity_id: &UUID_NIL }
    }

    pub fn get_team<'short, 'long: 'short>(&'long self, team_id: &'short Uuid) -> EntityView<'short> {
        EntityView { parent: self, entity_type: "team", entity_id: team_id }
    }

    pub fn get_game<'short, 'long: 'short>(&'long self, game_id: &'short Uuid) -> EntityView<'short> {
        EntityView { parent: self, entity_type: "game", entity_id: game_id }
    }

    pub fn get_player<'short, 'long: 'short>(&'long self, player_id: &'short Uuid) -> EntityView<'short> {
        EntityView { parent: self, entity_type: "player", entity_id: player_id }
    }

    pub fn games<'a>(&'a self) -> Result<impl Iterator<Item=OwningEntityView<'a>> + 'a, PathError> {
        let game_ids: Vec<Uuid> =  {
            let data = self.data.lock().unwrap();
            data.get("game")
                .ok_or_else(|| PathError::EntityTypeDoesNotExist("game"))?
                .keys()
                .cloned()
                .collect()
        };

        Ok(game_ids.into_iter().map(|game_id| {
            OwningEntityView { parent: self, entity_type: "game", entity_id: game_id }
        }))
    }
}

pub struct EntityView<'d> {
    parent: &'d DataView,
    entity_type: &'static str,
    pub entity_id: &'d Uuid,
}

pub struct OwningEntityView<'d> {
    parent: &'d DataView,
    entity_type: &'static str,
    entity_id: Uuid,
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
            components: vec![],
        }
    }

    fn caused_by(&self) -> Arc<Event> {
        self.parent.caused_by.clone()
    }
}

impl<'a> View<'a> for OwningEntityView<'a> {
    fn get_ref(&self) -> Result<MutexGuardRef<BlaseballData, Node>, PathError> {
        self.parent.get_ref()
            .try_map(|data| {
                data.get(self.entity_type)
                    .ok_or_else(|| PathError::EntityTypeDoesNotExist(self.entity_type))?
                    .get(&self.entity_id)
                    .ok_or_else(|| PathError::EntityDoesNotExist(self.entity_type, self.entity_id.clone()))
            })
    }

    fn get_ref_mut(&self) -> Result<MutexGuardRefMut<BlaseballData, Node>, PathError> {
        self.parent.get_ref_mut()
            .try_map_mut(|data| {
                data.get_mut(self.entity_type)
                    .ok_or_else(|| PathError::EntityTypeDoesNotExist(self.entity_type))?
                    .get_mut(&self.entity_id)
                    .ok_or_else(|| PathError::EntityDoesNotExist(self.entity_type, self.entity_id.clone()))
            })
    }

    fn get_path(&self) -> Path {
        Path {
            entity_type: self.entity_type,
            entity_id: Some(self.entity_id.clone()),
            components: vec![],
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

impl<'e> OwningEntityView<'e> {
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
                                value,
                            })?
                            .get(key)
                            .ok_or_else(|| PathError::MissingKey(self.get_path()))
                    }
                    PathComponentRef::Index(idx) => {
                        parent.as_array()
                            .map_err(|value| PathError::UnexpectedType {
                                path: self.get_path(),
                                expected_type: "object",
                                value,
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
                                value,
                            })?
                            .get_mut(key)
                            .ok_or_else(|| PathError::MissingKey(self.get_path()))
                    }
                    PathComponentRef::Index(idx) => {
                        parent.as_array_mut()
                            .map_err(|value| PathError::UnexpectedType {
                                path: self.get_path(),
                                expected_type: "object",
                                value,
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

    pub fn as_array(&self) -> Result<MutexGuardRef<BlaseballData, im::Vector<Node>>, PathError> {
        self.get_ref()?
            .try_map(|node| {
                node.as_array()
                    .map_err(|value| PathError::UnexpectedType {
                        path: self.get_path(),
                        expected_type: "array",
                        value,
                    })
            })
    }

    pub fn as_array_mut(&self) -> Result<MutexGuardRefMut<BlaseballData, im::Vector<Node>>, PathError> {
        self.get_ref_mut()?
            .try_map_mut(|node| {
                node.as_array_mut()
                    .map_err(|value| PathError::UnexpectedType {
                        path: self.get_path(),
                        expected_type: "array",
                        value,
                    })
            })
    }

    #[allow(dead_code)]
    pub fn as_object(&self) -> Result<MutexGuardRef<BlaseballData, im::HashMap<String, Node>>, PathError> {
        self.get_ref()?
            .try_map(|node| {
                node.as_object()
                    .map_err(|value| PathError::UnexpectedType {
                        path: self.get_path(),
                        expected_type: "object",
                        value,
                    })
            })
    }

    pub fn as_object_mut(&self) -> Result<MutexGuardRefMut<BlaseballData, im::HashMap<String, Node>>, PathError> {
        self.get_ref_mut()?
            .try_map_mut(|node| {
                node.as_object_mut()
                    .map_err(|value| PathError::UnexpectedType {
                        path: self.get_path(),
                        expected_type: "object",
                        value,
                    })
            })
    }

    pub fn as_int(&self) -> Result<i64, PathError> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().unwrap();

        let value = lock.value.as_int()
            .ok_or_else(|| self.path_error("int", &lock.value))?;

        Ok(value)
    }

    #[allow(dead_code)]
    pub fn as_float(&self) -> Result<f64, PathError> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().unwrap();

        let value = lock.value.as_float()
            .ok_or_else(|| self.path_error("float", &lock.value))?;

        Ok(value)
    }

    pub fn as_bool(&self) -> Result<bool, PathError> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().unwrap();

        let value = lock.value.as_bool()
            .ok_or_else(|| self.path_error("bool", &lock.value))?;

        Ok(value)
    }

    pub fn as_uuid(&self) -> Result<Uuid, PathError> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().unwrap();

        let value = lock.value.as_uuid()
            .ok_or_else(|| self.path_error("uuid", &lock.value))?;

        Ok(value)
    }

    pub fn as_string(&self) -> Result<String, PathError> {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().unwrap();

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

    pub fn set<T: Into<PrimitiveValue>>(&self, value: T) -> Result<(), PathError> {
        let mut node = self.get_ref_mut()?;
        *node = node.successor(value.into(), self.caused_by().clone());

        Ok(())
    }

    pub fn map_int<F, T>(&self, func: F) -> Result<(), PathError>
        where
            F: FnOnce(i64) -> T,
            T: Into<PrimitiveValue>
    {
        let int = self.as_int()?;
        let mut node = self.get_ref_mut()?;
        *node = node.successor(func(int).into(), self.caused_by().clone());

        Ok(())
    }

    #[allow(dead_code)]
    pub fn map_float<F, T>(&self, func: F) -> Result<(), PathError>
        where
            F: FnOnce(f64) -> T,
            T: Into<PrimitiveValue>
    {
        let float = self.as_float()?;
        let mut node = self.get_ref_mut()?;
        *node = node.successor(func(float).into(), self.caused_by().clone());

        Ok(())
    }

    pub fn map_float_range<F, T>(&self, func: F) -> Result<(), PathError>
        where
            F: FnOnce(f64, f64) -> T,
            T: Into<PrimitiveValue>
    {
        let primitive = self.as_primitive()?.clone();
        let lock = primitive.read().unwrap();

        let value = match lock.value {
            PrimitiveValue::Float(f) => { func(f, f) }
            PrimitiveValue::FloatRange(lower, upper) => { func(lower, upper) }
            _ => {
                return Err(PathError::UnexpectedType {
                    path: self.get_path(),
                    expected_type: "float or float range",
                    value: lock.value.to_string()
                })
            }
        };

        let mut node = self.get_ref_mut()?;
        *node = node.successor(value.into(), self.caused_by().clone());

        Ok(())
    }

    pub fn overwrite<T: Into<JsonValue>>(&self, value: T) -> Result<(), PathError> {
        let mut node = self.get_ref_mut()?;
        *node = Node::new_from_json(value.into(), self.caused_by());

        Ok(())
    }

    pub fn remove<'a, T: Into<PathComponentRef<'a>>>(&self, key: T) -> Result<(), PathError> {
        match key.into() {
            PathComponentRef::Key(key) => {
                let mut obj = self.as_object_mut()?;
                match obj.remove(key) {
                    None => { Err(PathError::MissingKey(self.get_path())) }
                    Some(_) => { Ok(()) }
                }
            }
            PathComponentRef::Index(idx) => {
                let mut arr = self.as_array_mut()?;
                match arr.get(idx) {
                    None => { Err(PathError::MissingKey(self.get_path())) }
                    Some(_) => {
                        arr.remove(idx);
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn push<'a, T: Into<PrimitiveValue>>(&self, value: T) -> Result<(), PathError> {
        let mut arr = self.as_array_mut()?;
        arr.push_back(Node::new_primitive(value.into(), self.caused_by()));

        Ok(())
    }

    pub fn pop_front(&self) -> Result<Option<Node>, PathError> {
        let mut arr = self.as_array_mut()?;

        Ok(arr.pop_front())
    }

    fn path_error<ValueT: Display>(&self, expected_type: &'static str, value: ValueT) -> PathError {
        PathError::UnexpectedType {
            path: self.get_path(),
            expected_type,
            value: value.to_string(),
        }
    }
}