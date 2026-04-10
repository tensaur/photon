use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::id::RunId;

use crate::ports::finalised::FinalisedStore;
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryFinalisedStore {
    data: Arc<DashMap<RunId, ()>>,
}

impl InMemoryFinalisedStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryFinalisedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FinalisedStore for InMemoryFinalisedStore {
    async fn mark_finalised(&self, run_id: &RunId) -> Result<(), WriteError> {
        self.data.insert(*run_id, ());
        Ok(())
    }

    async fn is_finalised(&self, run_id: &RunId) -> Result<bool, ReadError> {
        Ok(self.data.contains_key(run_id))
    }
}
