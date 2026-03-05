use photon_core::types::ack::AckStatus;
use photon_core::types::batch::AssembledBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::{BatchCodec, CodecError};
use photon_protocol::ports::compress::{CompressionError, Compressor};

use crate::domain::ingest::dedup::{
    DeduplicationError, DeduplicationTracker, DeduplicationVerdict,
};
use crate::domain::ports::metadata_store::MetadataStore;
use crate::domain::ports::metric_store::{MetricStore, MetricStoreError};

use bytes::BytesMut;

#[derive(Clone, Debug)]
pub struct IngestResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("deduplication check failed")]
    Dedup(#[from] DeduplicationError),

    #[error("decompression failed")]
    Decompress(#[from] CompressionError),

    #[error("batch decoding failed")]
    Decode(#[from] CodecError),

    #[error("metric store write failed")]
    Store(#[from] MetricStoreError),
}

pub trait IngestService: Clone + Send + Sync + 'static {
    fn ingest(&self, batch: &AssembledBatch) -> impl Future<Output = Result<IngestResult, IngestError>> + Send;
}

#[derive(Clone)]
pub struct Service<M, D, C, K>
where
    M: MetricStore,
    D: MetadataStore,
    C: Compressor,
    K: BatchCodec,
{
    store: M,
    dedup: DeduplicationTracker<D>,
    compressor: C,
    codec: K
}

impl<M, D, C, K> Service<M, D, C, K>
where
    M: MetricStore,
    D: MetadataStore,
    C: Compressor,
    K: BatchCodec,
{
    pub fn new(store: M, metadata: D, compressor: C, codec: K) -> Self {
        Self {
            store,
            dedup: DeduplicationTracker::new(metadata),
            compressor,
            codec,
        }
    }
}

impl<M, D, C, K> IngestService for Service<M, D, C, K>
where
    M: MetricStore,
    D: MetadataStore,
    C: Compressor,
    K: BatchCodec,
{
    /// Process a single assembled batch received from the SDK.
    /// 1. Dedup check — skip if already committed.
    /// 2. Verify CRC32.
    /// 3. Decompress payload.
    /// 4. Decode into MetricBatch.
    /// 5. Write to metric store.
    /// 6. Advance watermark.
    async fn ingest(&self, batch: &AssembledBatch) -> Result<IngestResult, IngestError> {
        let seq = batch.sequence_number;

        let verdict = self.dedup.check(&batch.run_id, seq).await?;
        if verdict == DeduplicationVerdict::Duplicate {
            return Ok(IngestResult {
                sequence_number: seq,
                status: AckStatus::Duplicate,
            });
        }

        let actual_crc = crc32fast::hash(&batch.compressed_payload);
        if actual_crc != batch.crc32 {
            tracing::warn!(
                sequence = u64::from(seq),
                expected_crc = batch.crc32,
                actual_crc,
                "CRC mismatch, rejecting batch"
            );
            return Ok(IngestResult {
                sequence_number: seq,
                status: AckStatus::Rejected,
            });
        }

        let mut decompress_buf = BytesMut::new();
        self.compressor
            .decompress(&batch.compressed_payload, &mut decompress_buf)?;

        let metric_batch = self.codec.decode(&decompress_buf)?;

        self.store.write_batch(&metric_batch).await?;
        self.dedup.advance(&batch.run_id, seq).await?;

        Ok(IngestResult {
            sequence_number: seq,
            status: AckStatus::Ok,
        })
    }
}
