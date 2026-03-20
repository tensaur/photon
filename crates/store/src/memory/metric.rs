use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};

use crate::ports::metric::{MetricReader, MetricWriter};
use crate::ports::{ReadError, WriteError};

#[derive(Clone)]
pub struct InMemoryMetricStore {
    data: Arc<DashMap<RunId, DashMap<Metric, BTreeMap<u64, f64>>>>,
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
            run.entry(batch.key(point).clone())
                .or_default()
                .insert(point.step, point.value);
        }
        Ok(())
    }
}

impl MetricReader for InMemoryMetricStore {
    async fn list_runs(&self) -> Result<Vec<RunId>, ReadError> {
        Ok(self.data.iter().map(|entry| *entry.key()).collect())
    }

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

        Ok(points.range(step_range).map(|(&s, &v)| (s, v)).collect())
    }

    async fn count_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<u64>,
    ) -> Result<usize, ReadError> {
        let Some(run) = self.data.get(run_id) else {
            return Ok(0);
        };
        let Some(points) = run.get(key) else {
            return Ok(0);
        };

        Ok(points.range(step_range).count())
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, ReadError> {
        let Some(run) = self.data.get(run_id) else {
            return Ok(Vec::new());
        };

        Ok(run.iter().map(|item| item.key().clone()).collect())
    }
}
