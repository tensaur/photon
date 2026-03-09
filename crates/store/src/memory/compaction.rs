use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;

use crate::ports::compaction::CompactionCursor;
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryCompactionCursor {
    data: Arc<DashMap<(RunId, Metric, usize), u64>>,
}

impl InMemoryCompactionCursor {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryCompactionCursor {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionCursor for InMemoryCompactionCursor {
    async fn get(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
    ) -> Result<Option<u64>, ReadError> {
        Ok(self.data.get(&(*run_id, key.clone(), tier)).map(|e| *e.value()))
    }

    async fn advance(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        offset: u64,
    ) -> Result<(), WriteError> {
        self.data.insert((*run_id, key.clone(), tier), offset);
        Ok(())
    }
}
