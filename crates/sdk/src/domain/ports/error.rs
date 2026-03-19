use std::time::Duration;

use photon_core::types::metric::MetricError;
use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::TransportError as InfraTransportError;

use crate::domain::flush::FlushError;
use crate::domain::send::{RecoveryError, SendError};

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

#[derive(Debug, thiserror::Error)]
pub enum SenderThreadError {
    #[error("failed to create sender runtime")]
    Runtime(#[source] std::io::Error),
    #[error("sender connection failed")]
    Connection(#[from] TransportError),
    #[error("WAL recovery failed")]
    Recovery(#[from] RecoveryError),
    #[error("sender run loop failed")]
    Sender(#[from] SendError),
}

/// SDK-specific transport error with domain variants.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection lost: {reason}")]
    ConnectionLost { reason: String },

    #[error("request timed out after {after:?}")]
    Timeout { after: Duration },

    #[error("batch {sequence} rejected by server: {message}")]
    Rejected {
        sequence: SequenceNumber,
        message: String,
    },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

impl From<InfraTransportError> for TransportError {
    fn from(e: InfraTransportError) -> Self {
        match e {
            InfraTransportError::Connection(msg)
            | InfraTransportError::Request(msg)
            | InfraTransportError::StreamClosed(msg) => Self::ConnectionLost { reason: msg },
            InfraTransportError::Unknown(e) => Self::Unknown(e),
        }
    }
}
