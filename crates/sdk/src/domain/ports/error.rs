use photon_core::types::metric::MetricError;

use crate::domain::pipeline::batch_builder::BatchBuilderError;
use crate::domain::pipeline::recovery::RecoveryError;
use crate::domain::pipeline::sender::SenderError;
use crate::domain::ports::transport::TransportError;

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("invalid metric key: {0}")]
    InvalidMetricKey(#[from] MetricError),
}

#[derive(Debug, thiserror::Error)]
pub enum FinishError {
    #[error("batch builder failed")]
    Builder(#[from] BatchBuilderError),
    #[error("sender failed")]
    Sender(#[from] SenderThreadError),
    #[error("pipeline thread panicked")]
    Panicked,
}

#[derive(Debug, thiserror::Error)]
pub enum SenderThreadError {
    #[error("failed to create sender runtime")]
    Runtime(#[source] std::io::Error),
    #[error("sender connection failed")]
    Connection(#[from] TransportError),
    #[error("WAL recovery failed")]
    Recovery(#[from] RecoveryError),
    #[error("sender run loop failed")]
    Sender(#[from] SenderError),
}
