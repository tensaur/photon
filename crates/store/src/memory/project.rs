use std::sync::Arc;

use dashmap::DashMap;

use photon_core::domain::project::Project;
use photon_core::types::id::ProjectId;

use crate::ports::{ReadError, ReadRepository, WriteError, WriteRepository};

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

impl ReadRepository<Project> for InMemoryProjectStore {
    async fn list(&self) -> Result<Vec<Project>, ReadError> {
        Ok(self
            .data
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get(&self, id: &ProjectId) -> Result<Option<Project>, ReadError> {
        Ok(self.data.get(id).map(|entry| entry.value().clone()))
    }
}

impl WriteRepository<Project> for InMemoryProjectStore {
    async fn upsert(&self, project: &Project) -> Result<(), WriteError> {
        self.data.insert(project.id, project.clone());
        Ok(())
    }
}
