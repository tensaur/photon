use std::future::Future;
use std::sync::{Arc, Mutex};

use photon_core::types::ack::AckStatus;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_core::types::wal::WalOffset;
use photon_store::ports::watermark::WatermarkReader;
use photon_wal::{Wal, WalAppender};

use crate::domain::dedup::{DeduplicationCache, Verdict};

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("WAL append failed")]
    Wal(#[from] photon_wal::WalError),

    #[error("sequence gap: expected contiguous stream but got seq {got} (session broken)")]
    SequenceGap { got: SequenceNumber },
}

pub trait IngestService: Clone + Send + Sync + 'static {
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
    wal: Arc<Mutex<A>>,
    notify: Arc<tokio::sync::Notify>,
}

impl<A: WalAppender> Clone for Service<A> {
    fn clone(&self) -> Self {
        Self {
            dedup: self.dedup.clone(),
            wal: Arc::clone(&self.wal),
            notify: Arc::clone(&self.notify),
        }
    }
}

impl<A: WalAppender> Service<A> {
    pub fn new(wal: A, notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            dedup: DeduplicationCache::new(),
            wal: Arc::new(Mutex::new(wal)),
            notify,
        }
    }

    pub async fn seed(&self, watermarks: &impl WatermarkReader, wal: &impl Wal) {
        let mut entries = watermarks.read_all().await.unwrap_or_default();
        if let Ok(tail) = wal.read_from(WalOffset::ZERO) {
            entries.extend(tail.iter().map(|b| (b.run_id, b.sequence_number)));
        }
        self.dedup.seed(&entries);
    }
}

impl<A: WalAppender> IngestService for Service<A> {
    async fn ingest(&self, batch: &WireBatch) -> Result<IngestResult, IngestError> {
        let seq = batch.sequence_number;

        // 1. Ordering check
        match self.dedup.check(&batch.run_id, seq) {
            Verdict::Duplicate => {
                return Ok(IngestResult {
                    sequence_number: seq,
                    status: AckStatus::Duplicate,
                });
            }
            Verdict::Gap => {
                return Err(IngestError::SequenceGap { got: seq });
            }
            Verdict::Process => {}
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
        self.wal.lock().unwrap().append(batch)?;

        // 4. Wake persist consumer
        self.notify.notify_one();

        // 5. Advance expected_next
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use photon_store::memory::watermark::InMemoryWatermarkStore;
    use photon_store::ports::watermark::WatermarkWriter;
    use photon_wal::WalAppender;

    use bytes::Bytes;

    use photon_core::types::ack::AckStatus;
    use photon_core::types::batch::WireBatch;
    use photon_core::types::id::RunId;
    use photon_core::types::sequence::SequenceNumber;
    use photon_wal::open_in_memory_wal;

    use super::{IngestError, IngestService, Service};

    fn make_batch(run_id: RunId, seq: u64, payload: &[u8]) -> WireBatch {
        let compressed_payload = Bytes::copy_from_slice(payload);
        let crc32 = crc32fast::hash(&compressed_payload);
        WireBatch {
            run_id,
            sequence_number: SequenceNumber::from(seq),
            compressed_payload,
            crc32,
            created_at: SystemTime::now(),
            point_count: 1,
            uncompressed_size: payload.len(),
        }
    }

    fn make_batch_bad_crc(run_id: RunId, seq: u64, payload: &[u8]) -> WireBatch {
        let compressed_payload = Bytes::copy_from_slice(payload);
        let crc32 = crc32fast::hash(&compressed_payload).wrapping_add(1);
        WireBatch {
            run_id,
            sequence_number: SequenceNumber::from(seq),
            compressed_payload,
            crc32,
            created_at: SystemTime::now(),
            point_count: 1,
            uncompressed_size: payload.len(),
        }
    }

    fn new_service() -> Service<photon_wal::InMemoryWalAppender> {
        let (appender, _mgr) = open_in_memory_wal();
        let notify = Arc::new(tokio::sync::Notify::new());
        Service::new(appender, notify)
    }

    #[tokio::test]
    async fn test_ingest_accepts_valid_batch() {
        let svc = new_service();
        let batch = make_batch(RunId::new(), 1, b"hello");

        let result = svc.ingest(&batch).await.expect("ingest should succeed");

        assert_eq!(result.status, AckStatus::Ok);
        assert_eq!(result.sequence_number, SequenceNumber::from(1));
    }

    #[tokio::test]
    async fn test_ingest_rejects_bad_crc() {
        let svc = new_service();
        let batch = make_batch_bad_crc(RunId::new(), 1, b"hello");

        let result = svc.ingest(&batch).await.expect("ingest should succeed");

        assert_eq!(result.status, AckStatus::Rejected);
    }

    #[tokio::test]
    async fn test_ingest_detects_duplicate() {
        let svc = new_service();
        let run_id = RunId::new();

        let batch = make_batch(run_id, 1, b"payload");
        svc.ingest(&batch).await.expect("first ingest");

        let second = svc.ingest(&batch).await.expect("second ingest");
        assert_eq!(second.status, AckStatus::Duplicate);
    }

    #[tokio::test]
    async fn test_ingest_detects_gap() {
        let svc = new_service();
        let run_id = RunId::new();

        // Skip seq 1, send seq 2
        let batch = make_batch(run_id, 2, b"payload");
        let result = svc.ingest(&batch).await;

        assert!(matches!(result, Err(IngestError::SequenceGap { .. })));
    }

    #[tokio::test]
    async fn test_sequential_batches_accepted() {
        let svc = new_service();
        let run_id = RunId::new();

        for seq in 1..=5 {
            let batch = make_batch(run_id, seq, b"data");
            let result = svc.ingest(&batch).await.expect("ingest should succeed");
            assert_eq!(result.status, AckStatus::Ok);
        }
    }

    #[tokio::test]
    async fn test_watermark_returns_highest_processed() {
        let svc = new_service();
        let run_id = RunId::new();

        for seq in 1..=3 {
            let batch = make_batch(run_id, seq, b"data");
            svc.ingest(&batch).await.expect("ingest should succeed");
        }

        let wm = svc.watermark(&run_id).await.expect("watermark");
        assert_eq!(u64::from(wm), 3);
    }

    #[tokio::test]
    async fn test_seed_sets_expected_next() {
        let svc = new_service();
        let run_id = RunId::new();
        let (_, wal_mgr) = open_in_memory_wal();

        // Watermark 5 = sequences 1-5 persisted, next expected is 6
        let wm_store = InMemoryWatermarkStore::new();
        wm_store
            .write_watermarks(&[(run_id, SequenceNumber::from(5))])
            .await
            .unwrap();
        svc.seed(&wm_store, &wal_mgr).await;

        let batch = make_batch(run_id, 6, b"data");
        let result = svc.ingest(&batch).await.expect("ingest");
        assert_eq!(result.status, AckStatus::Ok);
    }

    #[tokio::test]
    async fn test_seed_includes_wal_tail() {
        let svc = new_service();
        let run_id = RunId::new();

        // Ingest batches 1-3 (writes to WAL via the service's appender)
        for seq in 1..=3 {
            let batch = make_batch(run_id, seq, b"data");
            svc.ingest(&batch).await.expect("ingest");
        }

        // Simulate restart: new service, seed from the WAL that has batches 1-3
        let (appender, wal_mgr) = open_in_memory_wal();
        let new_svc = Service::new(appender, Arc::new(tokio::sync::Notify::new()));

        // Write the old batches into the new WAL to simulate unconsumed tail
        {
            let mut app = new_svc.wal.lock().unwrap();
            for seq in 1..=3 {
                app.append(&make_batch(run_id, seq, b"data")).unwrap();
            }
        }

        // Seed from empty watermarks + WAL tail
        let empty_wm = InMemoryWatermarkStore::new();
        new_svc.seed(&empty_wm, &wal_mgr).await;

        // Seq 3 should be duplicate, seq 4 accepted
        let batch = make_batch(run_id, 3, b"data");
        let result = new_svc.ingest(&batch).await.expect("ingest");
        assert_eq!(result.status, AckStatus::Duplicate);

        let batch = make_batch(run_id, 4, b"data");
        let result = new_svc.ingest(&batch).await.expect("ingest");
        assert_eq!(result.status, AckStatus::Ok);
    }

    #[tokio::test]
    async fn test_evict_run_resets() {
        let svc = new_service();
        let run_id = RunId::new();

        let batch = make_batch(run_id, 1, b"data");
        svc.ingest(&batch).await.expect("ingest");

        svc.evict_run(&run_id);

        let wm = svc.watermark(&run_id).await.expect("watermark");
        assert_eq!(u64::from(wm), 0);
    }
}
