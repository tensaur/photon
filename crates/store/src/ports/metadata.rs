use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

/// Tracks per-run deduplication watermarks.
pub trait MetadataStore: Clone + Send + Sync + 'static {
    fn get_watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Option<SequenceNumber>, MetadataError>> + Send;

    fn advance_watermark(
        &self,
        run_id: &RunId,
        sequence: SequenceNumber,
    ) -> impl Future<Output = Result<(), MetadataError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("run {0} not found")]
    RunNotFound(RunId),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
