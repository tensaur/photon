use photon_core::types::metric::MetricError;

use crate::domain::ports::error::{FinishError, LogError};
use crate::domain::ports::wal::WalError;

#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("invalid metric key: {0}")]
    InvalidMetricKey(MetricError),

    #[error("invalid config for {field}: {reason}")]
    ConfigInvalid { field: String, reason: String },

    #[error("WAL recovery failed: {0}")]
    WalRecoveryFailed(#[source] WalError),

    #[error("pipeline failed")]
    PipelineFailed(#[from] FinishError),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

impl From<LogError> for SdkError {
    fn from(e: LogError) -> Self {
        match e {
            LogError::InvalidMetricKey(me) => SdkError::InvalidMetricKey(me),
        }
    }
}
