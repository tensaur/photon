use std::future::Future;

use crate::types::id::RunId;
use crate::types::metric::MetricBatch;

pub trait MetricStore: Send + Sync + Clone + 'static {
    fn write_batch(
        &self,
        batch: &MetricBatch,
    ) -> impl Future<Output = Result<(), MetricStoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum MetricStoreError {
    #[error("invalid batch for run {run_id}: {reason}")]
    InvalidBatch {
        run_id: RunId,
        reason: String,
    },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
