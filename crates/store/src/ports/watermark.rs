use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use super::{ReadError, WriteError};

/// Per-run deduplication watermarks.
pub trait WatermarkStore: Send + Sync + Clone + 'static {
    fn get(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Option<SequenceNumber>, ReadError>> + Send;

    fn advance(
        &self,
        run_id: &RunId,
        seq: SequenceNumber,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}
