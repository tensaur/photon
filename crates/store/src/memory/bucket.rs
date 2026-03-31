use std::ops::Range;
use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::bucket::{Bucket, BucketEntry};
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};

use crate::ports::bucket::{BucketReader, BucketWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryBucketStore {
    data: Arc<DashMap<(RunId, Metric, usize), Vec<Bucket>>>,
}

impl InMemoryBucketStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryBucketStore {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketWriter for InMemoryBucketStore {
    async fn write_buckets(&self, entries: &[BucketEntry]) -> Result<(), WriteError> {
        for entry in entries {
            self.data
                .entry((entry.run_id, entry.key.clone(), entry.tier))
                .or_default()
                .push(entry.bucket.clone());
        }

        Ok(())
    }
}

impl BucketReader for InMemoryBucketStore {
    async fn read_buckets(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        step_range: Range<Step>,
    ) -> Result<Vec<Bucket>, ReadError> {
        let Some(buckets) = self.data.get(&(*run_id, key.clone(), tier)) else {
            return Ok(Vec::new());
        };

        Ok(buckets
            .iter()
            .filter(|b| b.step_start < step_range.end && b.step_end >= step_range.start)
            .cloned()
            .collect())
    }
}
