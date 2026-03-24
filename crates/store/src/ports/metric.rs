use std::future::Future;
use std::ops::Range;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};

use super::{ReadError, WriteError};

/// Append-only storage for raw metric points.
pub trait MetricWriter: Send + Sync + Clone + 'static {
    fn write_batch(
        &self,
        batch: &MetricBatch,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;

    fn write_batches<'a>(
        &'a self,
        batches: &'a [MetricBatch],
    ) -> impl Future<Output = Result<(), WriteError>> + Send + 'a {
        async move {
            for batch in batches {
                self.write_batch(batch).await?;
            }
            Ok(())
        }
    }
}

/// Range reads over raw metric points.
pub trait MetricReader: Send + Sync + Clone + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<RunId>, ReadError>> + Send;

    fn read_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<u64>,
    ) -> impl Future<Output = Result<Vec<(u64, f64)>, ReadError>> + Send;

    fn list_metrics(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, ReadError>> + Send;

    fn count_points(
        &self,
        run_id: &RunId,
        key: &Metric,
        step_range: Range<u64>,
    ) -> impl Future<Output = Result<usize, ReadError>> + Send;
}
