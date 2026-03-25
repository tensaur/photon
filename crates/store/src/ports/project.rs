use std::future::Future;

use photon_core::domain::project::Project;
use photon_core::types::id::ProjectId;

use super::{ReadError, WriteError};

/// Read access to project metadata.
pub trait ProjectReader: Send + Sync + Clone + 'static {
    fn list_projects(&self) -> impl Future<Output = Result<Vec<Project>, ReadError>> + Send;

    fn get_project(
        &self,
        id: &ProjectId,
    ) -> impl Future<Output = Result<Option<Project>, ReadError>> + Send;
}

/// Write access to project metadata.
pub trait ProjectWriter: Send + Sync + Clone + 'static {
    fn upsert_project(
        &self,
        project: &Project,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}
