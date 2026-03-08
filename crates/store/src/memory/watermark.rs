use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::ports::watermark::WatermarkStore;
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryWatermarkStore {
    data: Arc<DashMap<RunId, SequenceNumber>>,
}

impl InMemoryWatermarkStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryWatermarkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl WatermarkStore for InMemoryWatermarkStore {
    async fn get(&self, run_id: &RunId) -> Result<Option<SequenceNumber>, ReadError> {
        Ok(self.data.get(run_id).map(|entry| *entry.value()))
    }

    async fn advance(&self, run_id: &RunId, seq: SequenceNumber) -> Result<(), WriteError> {
        self.data.insert(*run_id, seq);
        Ok(())
    }
}
