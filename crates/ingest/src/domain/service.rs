use std::future::Future;
use std::sync::{Arc, Mutex};

use photon_core::types::ack::AckStatus;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_wal::WalAppender;

use crate::domain::dedup::{DeduplicationCache, Verdict};

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("WAL append failed")]
    Wal(#[source] photon_wal::WalError),
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
pub struct Service<A: WalAppender> {
    dedup: DeduplicationCache,
    wal: Mutex<A>,
    notify: Arc<tokio::sync::Notify>,
}

impl<A: WalAppender> Service<A> {
    pub fn new(wal: A, notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            dedup: DeduplicationCache::new(),
            wal: Mutex::new(wal),
            notify,
        }
    }

    pub fn seed_watermarks(&self, entries: &[(RunId, SequenceNumber)]) {
        self.dedup.seed(entries);
    }
}

impl<A: WalAppender> IngestService for Service<A> {
    async fn ingest(&self, batch: &WireBatch) -> Result<IngestResult, IngestError> {
        let seq = batch.sequence_number;

        // 1. Dedup check
        if self.dedup.check(&batch.run_id, seq) == Verdict::Duplicate {
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
            .map_err(IngestError::Wal)?;

        // 4. Wake persist consumer
        self.notify.notify_one();

        // 5. Advance dedup cache
        self.dedup.advance(&batch.run_id, seq);

        Ok(IngestResult {
            sequence_number: seq,
            status: AckStatus::Ok,
        })
    }

    async fn watermark(&self, run_id: &RunId) -> Result<SequenceNumber, IngestError> {
        Ok(self.dedup.watermark(run_id))
    }

    fn evict_run(&self, run_id: &RunId) {
        self.dedup.evict(run_id);
    }
}
