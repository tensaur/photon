use std::sync::Arc;

use dashmap::DashMap;

use photon_core::domain::experiment::Experiment;
use photon_core::types::id::ExperimentId;

use crate::ports::experiment::{ExperimentReader, ExperimentWriter};
use crate::ports::{ReadError, WriteError};

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

impl ExperimentReader for InMemoryExperimentStore {
    async fn list_experiments(&self) -> Result<Vec<Experiment>, ReadError> {
        Ok(self
            .data
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get_experiment(&self, id: &ExperimentId) -> Result<Option<Experiment>, ReadError> {
        Ok(self.data.get(id).map(|entry| entry.value().clone()))
    }
}

impl ExperimentWriter for InMemoryExperimentStore {
    async fn upsert_experiment(&self, experiment: &Experiment) -> Result<(), WriteError> {
        self.data.insert(experiment.id, experiment.clone());
        Ok(())
    }
}
