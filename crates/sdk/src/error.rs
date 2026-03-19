use photon_core::types::metric::MetricError;
use photon_flush::FlushError;
use photon_send::SenderThreadError;
use photon_wal::WalError;

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("WAL recovery failed")]
    Wal(#[from] WalError),

    #[error("invalid config for {field}: {reason}")]
    Config { field: String, reason: String },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("invalid metric key: {0}")]
    InvalidMetricKey(#[from] MetricError),
}

#[derive(Debug, thiserror::Error)]
pub enum FinishError {
    #[error("flush failed")]
    Flush(#[from] FlushError),
    #[error("sender failed")]
    Sender(#[from] SenderThreadError),
    #[error("pipeline thread panicked")]
    Panicked,
}
