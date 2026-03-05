use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;

pub trait MetricStore: Send + Sync + Clone + 'static {
    fn write_batch(
        &self,
        batch: &MetricBatch,
    ) -> impl Future<Output = Result<(), MetricStoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum MetricStoreError {
    #[error("invalid batch for run {run_id}: {reason}")]
    InvalidBatch { run_id: RunId, reason: String },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
