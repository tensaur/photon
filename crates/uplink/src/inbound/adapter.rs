use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::ingest::{IngestMessage, IngestResult};
use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::{Transport, TransportError as InfraTransportError};

use crate::domain::error::TransportError;
use crate::domain::ports::IngestConnection;

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

impl<T> IngestConnection for T
where
    T: Transport<IngestMessage, IngestResult>,
{
    async fn send_batch(&self, batch: &WireBatch) -> Result<(), TransportError> {
        self.send(&IngestMessage::Batch(batch.clone()))
            .await
            .map_err(Into::into)
    }

    async fn send_message(&self, msg: IngestMessage) -> Result<(), TransportError> {
        self.send(&msg).await.map_err(Into::into)
    }

    async fn query_watermark(&self, run_id: &RunId) -> Result<SequenceNumber, TransportError> {
        self.send(&IngestMessage::QueryWatermark(*run_id))
            .await
            .map_err(TransportError::from)?;

        match self.recv().await {
            Ok(IngestResult::Watermark(seq)) => Ok(seq),
            Ok(IngestResult::Error(e)) => {
                tracing::warn!("watermark query rejected by server: {e}");
                Ok(SequenceNumber::ZERO)
            }
            Ok(_) => Ok(SequenceNumber::ZERO),
            Err(e) => Err(e.into()),
        }
    }

    async fn recv(&self) -> Result<IngestResult, TransportError> {
        Transport::recv(self).await.map_err(Into::into)
    }
}
