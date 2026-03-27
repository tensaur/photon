use std::future::Future;

use photon_core::domain::Entity;

use super::{ReadError, WriteError};

/// Generic read access to entity metadata.
pub trait ReadRepository<T: Entity>: Send + Sync + Clone + 'static {
    fn list(&self) -> impl Future<Output = Result<Vec<T>, ReadError>> + Send;

    fn get(&self, id: &T::Id) -> impl Future<Output = Result<Option<T>, ReadError>> + Send;
}

/// Generic write access to entity metadata.
pub trait WriteRepository<T: Entity>: Send + Sync + Clone + 'static {
    fn upsert(&self, entity: &T) -> impl Future<Output = Result<(), WriteError>> + Send;
}
