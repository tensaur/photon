use std::time::Duration;

use photon_core::types::sequence::SequenceNumber;
use photon_wal::ports::WalError;

#[derive(Debug, thiserror::Error)]
pub enum UplinkTransportError {
    #[error("connection lost: {reason}")]
    ConnectionLost { reason: String },

    #[error("request timed out after {after:?}")]
    Timeout { after: Duration },

    #[error("batch {sequence} rejected by server: {message}")]
    Rejected {
        sequence: SequenceNumber,
        message: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum UplinkError {
    #[error("WAL operation failed")]
    Wal(#[from] WalError),

    #[error("transport error")]
    Transport(#[from] UplinkTransportError),

    #[error("shutdown timeout after {0:?}")]
    ShutdownTimeout(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("WAL error during recovery")]
    Wal(#[from] WalError),

    #[error("transport error during recovery")]
    Transport(#[from] UplinkTransportError),
}
