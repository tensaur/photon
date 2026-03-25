use std::future::Future;

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::ingest::{IngestMessage, IngestResult};
use photon_core::types::sequence::SequenceNumber;

use super::error::TransportError;

pub trait IngestConnection: Clone + Send + Sync + 'static {
    fn send_batch(
        &self,
        batch: &WireBatch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn send_message(
        &self,
        msg: IngestMessage,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn query_watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<SequenceNumber, TransportError>> + Send;

    fn recv(&self) -> impl Future<Output = Result<IngestResult, TransportError>> + Send;
}
