use std::future::Future;

use crate::types::id::RunId;
use crate::types::sequence::SequenceNumber;

pub trait MetadataStore: Send + Sync + Clone + 'static {
    fn get_watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Option<SequenceNumber>, MetadataStoreError>> + Send;

    fn advance_watermark(
        &self,
        run_id: &RunId,
        sequence: SequenceNumber,
    ) -> impl Future<Output = Result<(), MetadataStoreError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataStoreError {
    #[error("run {0} not found")]
    RunNotFound(RunId),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
