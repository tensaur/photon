use std::collections::HashMap;

use crate::ports::metadata_store::{MetadataStore, MetadataStoreError};
use crate::types::id::RunId;
use crate::types::sequence::SequenceNumber;

pub struct DeduplicationTracker<D: MetadataStore> {
    metadata: D,
    watermarks: HashMap<RunId, SequenceNumber>,
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
            watermarks: HashMap::new(),
        }
    }

    /// Check whether a batch should be processed or skipped.
    pub async fn check(
        &mut self,
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
        &mut self,
        run_id: &RunId,
        sequence: SequenceNumber,
    ) -> Result<(), DeduplicationError> {
        self.metadata.advance_watermark(run_id, sequence).await?;
        self.watermarks.insert(run_id.clone(), sequence);
        Ok(())
    }

    async fn get_watermark(
        &mut self,
        run_id: &RunId,
    ) -> Result<SequenceNumber, DeduplicationError> {
        if let Some(&watermark) = self.watermarks.get(run_id) {
            return Ok(watermark);
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
