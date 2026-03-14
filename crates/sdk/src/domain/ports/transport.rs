use std::future::Future;
use std::time::Duration;

use photon_core::types::ack::AckResult;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

/// Abstraction over the batch transport layer.
pub trait BatchTransport: Send + Sync + Clone + 'static {
    fn send(
        &self,
        batch: &WireBatch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn recv_ack(&self) -> impl Future<Output = Result<AckResult, TransportError>> + Send;

    fn get_watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<SequenceNumber, TransportError>> + Send;
}

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
