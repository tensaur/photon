use std::sync::Arc;

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::WireBatch;
use photon_transport::ports::{Transport, TransportError};

use crate::domain::service::IngestService;

/// Transport-agnostic ingest handler.
pub async fn handle_stream<S, T>(service: &Arc<S>, transport: &T)
where
    S: IngestService,
    T: Transport<AckResult, WireBatch>,
{
    loop {
        let batch = match transport.recv().await {
            Ok(batch) => batch,
            Err(TransportError::StreamClosed(_)) => break,
            Err(e) => {
                tracing::warn!("stream receive error: {e}");
                break;
            }
        };

        let ack = match service.ingest(&batch).await {
            Ok(result) => AckResult {
                sequence_number: result.sequence_number,
                status: result.status,
            },
            Err(e) => {
                tracing::error!("ingest error: {e}");
                AckResult {
                    sequence_number: batch.sequence_number,
                    status: AckStatus::Rejected,
                }
            }
        };

        if transport.send(&ack).await.is_err() {
            break;
        }
    }
}
