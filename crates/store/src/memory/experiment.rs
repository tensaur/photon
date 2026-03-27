use std::sync::Arc;

use dashmap::DashMap;

use photon_core::domain::experiment::Experiment;
use photon_core::types::id::ExperimentId;

use crate::ports::{ReadError, ReadRepository, WriteError, WriteRepository};

#[derive(Clone)]
pub struct InMemoryExperimentStore {
    data: Arc<DashMap<ExperimentId, Experiment>>,
}

impl InMemoryExperimentStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryExperimentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRepository<Experiment> for InMemoryExperimentStore {
    async fn list(&self) -> Result<Vec<Experiment>, ReadError> {
        Ok(self
            .data
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get(&self, id: &ExperimentId) -> Result<Option<Experiment>, ReadError> {
        Ok(self.data.get(id).map(|entry| entry.value().clone()))
    }
}

impl WriteRepository<Experiment> for InMemoryExperimentStore {
    async fn upsert(&self, experiment: &Experiment) -> Result<(), WriteError> {
        self.data.insert(experiment.id, experiment.clone());
        Ok(())
    }
}
