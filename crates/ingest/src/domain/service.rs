use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use photon_core::types::ack::AckStatus;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_store::ports::watermark::WatermarkStore;
use photon_wal::WalAppender;

use crate::domain::dedup::{DeduplicationError, DeduplicationTracker, Verdict};

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("deduplication failed")]
    Dedup(#[from] DeduplicationError),

    #[error("WAL append failed")]
    Wal(#[source] photon_store::ports::WriteError),
}

pub trait IngestService: Send + Sync + 'static {
    fn ingest(
        &self,
        batch: &WireBatch,
    ) -> impl Future<Output = Result<IngestResult, IngestError>> + Send;

    fn watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<SequenceNumber, IngestError>> + Send;

    fn evict_run(&self, run_id: &RunId);
}

/// WAL-backed ingest service.
///
/// Hot path: dedup check → CRC verify → WAL append → notify consumer → ack.
/// No decompression, no decoding, no ClickHouse on the hot path.
pub struct Service<W: WatermarkStore, A: WalAppender> {
    dedup: DeduplicationTracker<W>,
    wal: Mutex<A>,
    notify: Arc<tokio::sync::Notify>,
}

impl<W: WatermarkStore, A: WalAppender> Service<W, A> {
    pub fn new(watermark_store: W, wal: A, notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            dedup: DeduplicationTracker::new(watermark_store),
            wal: Mutex::new(wal),
            notify,
        }
    }

    /// Seed the dedup cache with watermarks loaded from the WAL meta file.
    pub fn seed_watermarks(&self, watermarks: &HashMap<RunId, SequenceNumber>) {
        for (run_id, seq) in watermarks {
            self.dedup.advance_local(run_id, *seq);
        }
    }
}

impl<W: WatermarkStore, A: WalAppender> IngestService for Service<W, A> {
    async fn ingest(&self, batch: &WireBatch) -> Result<IngestResult, IngestError> {
        let seq = batch.sequence_number;

        // 1. Dedup check
        if self.dedup.check(&batch.run_id, seq).await? == Verdict::Duplicate {
            return Ok(IngestResult {
                sequence_number: seq,
                status: AckStatus::Duplicate,
            });
        }

        // 2. CRC verify
        let actual_crc = crc32fast::hash(&batch.compressed_payload);
        if actual_crc != batch.crc32 {
            return Ok(IngestResult {
                sequence_number: seq,
                status: AckStatus::Rejected,
            });
        }

        // 3. WAL append
        self.wal
            .lock()
            .unwrap()
            .append(batch)
            .map_err(|e| IngestError::Wal(photon_store::ports::WriteError::Unknown(e.into())))?;

        // 4. Wake flush consumer
        self.notify.notify_one();

        // 5. Advance in-memory dedup cache
        self.dedup.advance_local(&batch.run_id, seq);

        Ok(IngestResult {
            sequence_number: seq,
            status: AckStatus::Ok,
        })
    }

    async fn watermark(&self, run_id: &RunId) -> Result<SequenceNumber, IngestError> {
        Ok(self.dedup.watermark(run_id).await?)
    }

    fn evict_run(&self, run_id: &RunId) {
        self.dedup.evict(run_id);
    }
}
