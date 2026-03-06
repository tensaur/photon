use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};

pub trait MetricWriter: Clone + Send + Sync + 'static {
    fn write_batch(
        &self,
        batch: &MetricBatch,
    ) -> impl Future<Output = Result<(), MetricWriteError>> + Send;
}

pub trait MetricReader: Clone + Send + Sync + 'static {
    /// Return all points for a run, ordered by step ascending.
    fn read_metrics(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<MetricPoint>, MetricReadError>> + Send;

    /// Return the distinct metric keys recorded for a run.
    fn list_metric_keys(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, MetricReadError>> + Send;
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
