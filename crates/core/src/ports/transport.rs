use std::future::Future;
use std::time::Duration;

use crate::types::batch::AssembledBatch;
use crate::types::id::RunId;
use crate::types::sequence::SequenceNumber;

/// Abstraction over the batch transport layer.
pub trait BatchTransport: Send + Sync + Clone + 'static {
    fn send(
        &self,
        batch: &AssembledBatch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn recv_ack(&self) -> impl Future<Output = Result<AckResult, TransportError>> + Send;

    fn get_watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<SequenceNumber, TransportError>> + Send;
}

/// Domain representation of a server acknowledgment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckStatus {
    Ok,
    Duplicate,
    Rejected,
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
