use std::future::Future;
use std::time::SystemTime;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};

pub trait MetricWriter: Clone + Send + Sync + 'static {
    fn write_batch(
        &self,
        batch: &MetricBatch,
    ) -> impl Future<Output = Result<(), MetricWriteError>> + Send;
}

/// Filter parameters are primitives that map to storage operations
/// like WHERE clauses. The query domain builds richer concepts on top.
pub trait MetricReader: Clone + Send + Sync + 'static {
    /// Return all points matching the filter, ordered by step.
    fn read_metrics(
        &self,
        run_id: &RunId,
        filter: &MetricFilter,
    ) -> impl Future<Output = Result<Vec<MetricPoint>, MetricReadError>> + Send;

    /// Return the distinct metric keys recorded for a run.
    fn list_metric_keys(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, MetricReadError>> + Send;
}

/// Primitive storage-level filters.
#[derive(Clone, Debug, Default)]
pub struct MetricFilter {
    pub keys: Option<Vec<Metric>>,
    pub step_min: Option<u64>,
    pub step_max: Option<u64>,
    pub time_min: Option<SystemTime>,
    pub time_max: Option<SystemTime>,
    pub limit: Option<usize>,
}

#[derive(Debug, thiserror::Error)]
pub enum MetricWriteError {
    #[error("invalid batch for run {run_id}: {reason}")]
    InvalidBatch { run_id: RunId, reason: String },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum MetricReadError {
    #[error("run {0} not found")]
    RunNotFound(RunId),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
