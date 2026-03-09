use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_store::ports::watermark::WatermarkStore;
use photon_store::ports::{ReadError, WriteError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Process,
    Duplicate,
}

#[derive(Debug, thiserror::Error)]
pub enum DeduplicationError {
    #[error("failed to read watermark")]
    Read(#[from] ReadError),

    #[error("failed to advance watermark")]
    Write(#[source] WriteError),
}

/// Wraps a [`WatermarkStore`] with an in-memory cache to avoid
/// hitting the store on every dedup check.
pub struct DeduplicationTracker<W: WatermarkStore> {
    store: W,
    cache: DashMap<RunId, SequenceNumber>,
}

impl<W: WatermarkStore> DeduplicationTracker<W> {
    pub fn new(store: W) -> Self {
        Self {
            store,
            cache: DashMap::new(),
        }
    }

    pub async fn check(
        &self,
        run_id: &RunId,
        seq: SequenceNumber,
    ) -> Result<Verdict, DeduplicationError> {
        let watermark = self.watermark(run_id).await?;

        if seq <= watermark {
            Ok(Verdict::Duplicate)
        } else {
            Ok(Verdict::Process)
        }
    }

    pub async fn advance(
        &self,
        run_id: &RunId,
        seq: SequenceNumber,
    ) -> Result<(), DeduplicationError> {
        self.store
            .advance(run_id, seq)
            .await
            .map_err(DeduplicationError::Write)?;

        self.cache.insert(*run_id, seq);
        Ok(())
    }

    pub async fn watermark(&self, run_id: &RunId) -> Result<SequenceNumber, DeduplicationError> {
        if let Some(w) = self.cache.get(run_id) {
            return Ok(*w.value());
        }

        let w = self
            .store
            .get(run_id)
            .await?
            .unwrap_or(SequenceNumber::ZERO);

        self.cache.insert(*run_id, w);
        Ok(w)
    }

    pub fn evict(&self, run_id: &RunId) {
        self.cache.remove(run_id);
    }
}
