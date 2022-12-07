use thiserror::Error;
use uuid::Uuid;
use crate::state::EntityType;

#[derive(Debug, Clone, Error)]
pub enum IngestError {
    #[error("Tried to apply event to entity {id} of type {ty}, but that entity does not exist")]
    EntityDoesNotExist { ty: EntityType, id: Uuid}
}

pub type IngestResult<T> = Result<T, IngestError>;