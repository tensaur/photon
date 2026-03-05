use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::domain::ports::metadata_store::{MetadataStore, MetadataStoreError};

#[derive(Clone)]
pub struct DeduplicationTracker<D: MetadataStore> {
    metadata: D,
    watermarks: DashMap<RunId, SequenceNumber>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeduplicationVerdict {
    Process,
    Duplicate,
}

#[derive(Debug, thiserror::Error)]
pub enum DeduplicationError {
    #[error("metadata store error")]
    Metadata(#[from] MetadataStoreError),
}

impl<D: MetadataStore> DeduplicationTracker<D> {
    pub fn new(metadata: D) -> Self {
        Self {
            metadata,
            watermarks: DashMap::new(),
        }
    }

    /// Check whether a batch should be processed or skipped.
    pub async fn check(
        &self,
        run_id: &RunId,
        sequence: SequenceNumber,
    ) -> Result<DeduplicationVerdict, DeduplicationError> {
        let watermark = self.get_watermark(run_id).await?;

        if sequence <= watermark {
            Ok(DeduplicationVerdict::Duplicate)
        } else {
            Ok(DeduplicationVerdict::Process)
        }
    }

    pub async fn advance(
        &self,
        run_id: &RunId,
        sequence: SequenceNumber,
    ) -> Result<(), DeduplicationError> {
        self.metadata.advance_watermark(run_id, sequence).await?;
        self.watermarks.insert(run_id.clone(), sequence);
        Ok(())
    }

    async fn get_watermark(&self, run_id: &RunId) -> Result<SequenceNumber, DeduplicationError> {
        if let Some(watermark) = self.watermarks.get(run_id) {
            return Ok(*watermark.value());
        }

        let watermark = self
            .metadata
            .get_watermark(run_id)
            .await?
            .unwrap_or(SequenceNumber::ZERO);

        self.watermarks.insert(run_id.clone(), watermark);
        Ok(watermark)
    }
}
