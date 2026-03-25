use std::sync::Arc;

use dashmap::DashMap;

use photon_core::domain::run::Run;
use photon_core::types::id::RunId;

use crate::ports::run::{RunReader, RunWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryRunStore {
    data: Arc<DashMap<RunId, Run>>,
}

impl InMemoryRunStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryRunStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RunReader for InMemoryRunStore {
    async fn list_runs(&self) -> Result<Vec<Run>, ReadError> {
        Ok(self
            .data
            .iter()
            .map(|entry| entry.value().clone())
            .collect())
    }

    async fn get_run(&self, run_id: &RunId) -> Result<Option<Run>, ReadError> {
        Ok(self.data.get(run_id).map(|entry| entry.value().clone()))
    }
}

impl RunWriter for InMemoryRunStore {
    async fn upsert_run(&self, run: &Run) -> Result<(), WriteError> {
        self.data.insert(run.id, run.clone());
        Ok(())
    }
}
