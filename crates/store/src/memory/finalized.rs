use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::id::RunId;

use crate::ports::finalized::FinalizedStore;
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryFinalizedStore {
    data: Arc<DashMap<RunId, ()>>,
}

impl InMemoryFinalizedStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryFinalizedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FinalizedStore for InMemoryFinalizedStore {
    async fn mark_finalized(&self, run_id: &RunId) -> Result<(), WriteError> {
        self.data.insert(*run_id, ());
        Ok(())
    }

    async fn is_finalized(&self, run_id: &RunId) -> Result<bool, ReadError> {
        Ok(self.data.contains_key(run_id))
    }
}
