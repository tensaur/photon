use photon_core::types::metric::MetricError;

use crate::domain::ports::wal::WalError;

#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("pipeline has shut down")]
    PipelineShutdown,

    #[error("invalid metric key: {0}")]
    InvalidMetricKey(#[from] MetricError),

    #[error("invalid config for {field}: {reason}")]
    ConfigInvalid { field: String, reason: String },

    #[error("WAL recovery failed: {0}")]
    WalRecoveryFailed(#[source] WalError),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
