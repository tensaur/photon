use std::collections::HashMap;
use std::future::Future;

use bytes::BytesMut;

use photon_core::types::ack::AckStatus;
use photon_core::types::batch::AssembledBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_core::types::metric::MetricBatch;
use photon_downsample::ports::aggregator::Aggregator;
use photon_downsample::reducer::BatchReducer;
use photon_hook::{IngestHook};
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::bucket::BucketWriter;
use photon_store::ports::metric::MetricWriter;
use photon_store::ports::watermark::WatermarkStore;

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

    #[error("decompression failed")]
    Decompress(#[source] photon_protocol::ports::compress::CompressionError),

    #[error("batch decoding failed")]
    Decode(#[source] photon_protocol::ports::codec::CodecError),

    #[error("metric store write failed")]
    MetricWrite(#[source] photon_store::ports::WriteError),

    #[error("bucket store write failed")]
    BucketWrite(#[source] photon_store::ports::WriteError),
}

pub trait IngestService {
    fn ingest(
        &mut self,
        batch: &AssembledBatch,
    ) -> impl Future<Output = Result<IngestResult, IngestError>> + Send;

    fn watermark(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<SequenceNumber, IngestError>> + Send;

    fn evict_run(&mut self, run_id: &RunId);
}

pub struct Service<A, W, M, B, H, C, K>
where
    A: Aggregator,
    W: WatermarkStore,
    M: MetricWriter,
    B: BucketWriter,
    H: IngestHook,
    C: Compressor,
    K: Codec<MetricBatch>,
{
    dedup: DeduplicationTracker<W>,
    aggregator: A,
    tier_widths: Vec<u64>,
    reducers: HashMap<RunId, BatchReducer<A>>,
    metric_store: M,
    bucket_store: B,
    hook: H,
    compressor: C,
    codec: K,
}

impl<A, W, M, B, H, C, K> Service<A, W, M, B, H, C, K>
where
    A: Aggregator,
    W: WatermarkStore,
    M: MetricWriter,
    B: BucketWriter,
    H: IngestHook,
    C: Compressor,
    K: Codec<MetricBatch>,
{
    pub fn new(
        aggregator: A,
        tier_widths: Vec<u64>,
        watermark_store: W,
        metric_store: M,
        bucket_store: B,
        hook: H,
        compressor: C,
        codec: K,
    ) -> Self {
        Self {
            dedup: DeduplicationTracker::new(watermark_store),
            aggregator,
            tier_widths,
            reducers: HashMap::new(),
            metric_store,
            bucket_store,
            hook,
            compressor,
            codec,
        }
    }
}

impl<A, W, M, B, H, C, K> IngestService for Service<A, W, M, B, H, C, K>
where
    A: Aggregator,
    W: WatermarkStore,
    M: MetricWriter,
    B: BucketWriter,
    H: IngestHook,
    C: Compressor,
    K: Codec<MetricBatch>,
{
    async fn ingest(&mut self, batch: &AssembledBatch) -> Result<IngestResult, IngestError> {
        let seq = batch.sequence_number;

        // 1. Dedup
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

        // 3. Decompress + decode
        let mut buf = BytesMut::new();
        self.compressor
            .decompress(&batch.compressed_payload, &mut buf)
            .map_err(IngestError::Decompress)?;
        let metric_batch = self.codec.decode(&buf).map_err(IngestError::Decode)?;

        // 4. Write raw points
        self.metric_store
            .write_batch(&metric_batch)
            .await
            .map_err(IngestError::MetricWrite)?;

        // 5. Downsample
        let reducer = self.reducers.entry(batch.run_id).or_insert_with(|| {
            BatchReducer::new(self.aggregator.clone(), self.tier_widths.clone())
        });
        let entries = reducer.ingest(&metric_batch);

        // 6. Write closed buckets
        if !entries.is_empty() {
            self.bucket_store
                .write_buckets(&batch.run_id, &entries)
                .await
                .map_err(IngestError::BucketWrite)?;
        }

        // 7. Notify hooks
        self.hook.on_batch_decoded(batch.run_id, &metric_batch);
        for entry in &entries {
            self.hook
                .on_buckets_closed(batch.run_id, &entry.key, entry.tier, &entry.bucket);
        }

        // 8. Advance watermark
        self.dedup.advance(&batch.run_id, seq).await?;

        Ok(IngestResult {
            sequence_number: seq,
            status: AckStatus::Ok,
        })
    }

    async fn watermark(
        &self,
        run_id: &RunId,
    ) -> Result<SequenceNumber, IngestError> {
        Ok(self.dedup.watermark(run_id).await?)
    }

    fn evict_run(&mut self, run_id: &RunId) {
        self.reducers.remove(run_id);
        self.dedup.evict(run_id);
    }
}

