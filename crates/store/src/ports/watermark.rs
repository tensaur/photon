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

    fn advance_many<'a>(
        &'a self,
        entries: &'a [(RunId, SequenceNumber)],
    ) -> impl Future<Output = Result<(), WriteError>> + Send + 'a {
        async move {
            for (run_id, seq) in entries {
                self.advance(run_id, *seq).await?;
            }
            Ok(())
        }
    }
}
