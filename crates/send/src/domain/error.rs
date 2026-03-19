use std::time::Duration;

use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::TransportError as InfraTransportError;
use photon_wal::ports::WalError;

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

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("WAL operation failed")]
    Wal(#[from] WalError),

    #[error("transport error")]
    Transport(#[from] TransportError),

    #[error("shutdown timeout after {0:?}")]
    ShutdownTimeout(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("WAL error during recovery")]
    Wal(#[from] WalError),

    #[error("transport error during recovery")]
    Transport(#[from] TransportError),
}
