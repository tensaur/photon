use std::sync::Arc;

use dashmap::DashMap;

use photon_core::domain::project::Project;
use photon_core::types::id::ProjectId;

use crate::ports::project::{ProjectReader, ProjectWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryProjectStore {
    data: Arc<DashMap<ProjectId, Project>>,
}

impl InMemoryProjectStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryProjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectReader for InMemoryProjectStore {
    async fn list_projects(&self) -> Result<Vec<Project>, ReadError> {
        Ok(self
            .data
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get_project(&self, id: &ProjectId) -> Result<Option<Project>, ReadError> {
        Ok(self.data.get(id).map(|entry| entry.value().clone()))
    }
}

impl ProjectWriter for InMemoryProjectStore {
    async fn upsert_project(&self, project: &Project) -> Result<(), WriteError> {
        self.data.insert(project.id, project.clone());
        Ok(())
    }
}
