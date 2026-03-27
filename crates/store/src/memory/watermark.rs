use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::ports::watermark::{WatermarkReader, WatermarkWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryWatermarkStore {
    inner: DashMap<RunId, SequenceNumber>,
}

impl Default for InMemoryWatermarkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryWatermarkStore {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }
}

impl WatermarkWriter for InMemoryWatermarkStore {
    async fn write_watermarks(
        &self,
        entries: &[(RunId, SequenceNumber)],
    ) -> Result<(), WriteError> {
        for (run_id, seq) in entries {
            self.inner
                .entry(*run_id)
                .and_modify(|existing| {
                    if *seq > *existing {
                        *existing = *seq;
                    }
                })
                .or_insert(*seq);
        }
        Ok(())
    }
}

impl WatermarkReader for InMemoryWatermarkStore {
    async fn read_all(&self) -> Result<Vec<(RunId, SequenceNumber)>, ReadError> {
        Ok(self
            .inner
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect())
    }
}
