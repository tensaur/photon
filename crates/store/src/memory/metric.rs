use std::ops::Range;
use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};

use crate::ports::metric::{MetricReader, MetricWriter};
use crate::ports::{ReadError, WriteError};

/// In-memory raw metric storage. Useful for testing.
#[derive(Clone)]
pub struct InMemoryMetricStore {
    data: Arc<DashMap<RunId, DashMap<Metric, Vec<(u64, f64)>>>>,
}

impl InMemoryMetricStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
        }
    }
}

impl Default for InMemoryMetricStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricWriter for InMemoryMetricStore {
    async fn write_batch(&self, batch: &MetricBatch) -> Result<(), WriteError> {
        let run = self.data.entry(batch.run_id).or_default();
        for point in &batch.points {
            run.entry(point.key.clone())
                .or_default()
                .push((point.step, point.value));
        }
        Ok(())
    }
}

impl MetricReader for InMemoryMetricStore {
    async fn read_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<u64>,
    ) -> Result<Vec<(u64, f64)>, ReadError> {
        let Some(run) = self.data.get(run_id) else {
            return Ok(Vec::new());
        };
        let Some(points) = run.get(key) else {
            return Ok(Vec::new());
        };

        Ok(points
            .iter()
            .filter(|(step, _)| *step >= step_range.start && *step < step_range.end)
            .copied()
            .collect())
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, ReadError> {
        let Some(run) = self.data.get(run_id) else {
            return Ok(Vec::new());
        };

        Ok(run.iter().map(|item| item.key().clone()).collect())
    }
}

