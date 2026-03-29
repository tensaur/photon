use photon_batch::BatchError;
use photon_core::types::metric::MetricError;
use photon_uplink::UplinkThreadError;
use photon_wal::WalError;

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("WAL recovery failed")]
    Wal(#[from] WalError),

    #[error("invalid config for {field}: {reason}")]
    Config { field: String, reason: String },

    #[error("failed to spawn thread: {0}")]
    ThreadSpawn(String),
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("invalid metric key: {0}")]
    InvalidMetricKey(#[from] MetricError),

    #[error("step {step} is not monotonically increasing for metric {key} (last step was {last})")]
    StepNotMonotonic { key: String, step: u64, last: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum FinishError {
    #[error("batch failed")]
    Batch(#[from] BatchError),
    #[error("uplink failed")]
    Uplink(#[from] UplinkThreadError),
    #[error("pipeline thread panicked")]
    Panicked,
}
