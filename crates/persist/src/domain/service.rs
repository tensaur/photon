use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::sync::broadcast;

use photon_core::types::batch::WireBatch;
use photon_core::types::bucket::BucketEntry;
use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::bucket::BucketWriter;
use photon_store::ports::metric::MetricWriter;
use photon_store::ports::watermark::WatermarkWriter;

use photon_downsample::aggregator::AggregatorKind;

use super::projections::Projection;
use super::projections::downsample::DownsampleProjection;

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("decompression failed")]
    Decompress(#[source] photon_protocol::ports::compress::CompressionError),

    #[error("decode failed")]
    Decode(#[source] photon_protocol::ports::codec::CodecError),

    #[error("metric write failed")]
    MetricWrite(#[source] photon_store::ports::WriteError),

    #[error("watermark write failed")]
    WatermarkWrite(#[source] photon_store::ports::WriteError),

    #[error("bucket write failed")]
    BucketWrite(#[source] photon_store::ports::WriteError),
}

#[derive(Clone)]
pub struct PersistConfig {
    pub downsample_widths: Vec<u64>,
    pub poll_interval: std::time::Duration,
    pub max_batch_read: usize,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            downsample_widths: vec![100, 1000, 10000],
            poll_interval: std::time::Duration::from_millis(100),
            max_batch_read: 1000,
        }
    }
}

pub trait PersistService: Clone + Send + Sync + 'static {
    fn write(&self, batches: &[WireBatch])
    -> impl Future<Output = Result<(), PersistError>> + Send;
}

#[derive(Clone)]
pub struct Service<C, K, M, W, B>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
{
    compressor: C,
    codec: K,
    metric_writer: M,
    watermark_writer: W,
    bucket_writer: B,
    event_tx: broadcast::Sender<PhotonEvent>,
    downsample: Arc<Mutex<DownsampleProjection<AggregatorKind>>>,
}

impl<C, K, M, W, B> Service<C, K, M, W, B>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
{
    pub fn new(
        compressor: C,
        codec: K,
        metric_writer: M,
        watermark_writer: W,
        bucket_writer: B,
        event_tx: broadcast::Sender<PhotonEvent>,
        config: PersistConfig,
    ) -> Self {
        let downsample =
            DownsampleProjection::new(AggregatorKind::default(), config.downsample_widths);
        Self {
            compressor,
            codec,
            metric_writer,
            watermark_writer,
            bucket_writer,
            event_tx,
            downsample: Arc::new(Mutex::new(downsample)),
        }
    }
}

impl<C, K, M, W, B> PersistService for Service<C, K, M, W, B>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
{
    async fn write(&self, batches: &[WireBatch]) -> Result<(), PersistError> {
        let mut decoded_batches = Vec::with_capacity(batches.len());
        let mut watermarks: HashMap<RunId, SequenceNumber> = HashMap::new();

        // Phase 1: Decode
        for batch in batches {
            let mut buf = BytesMut::with_capacity(batch.uncompressed_size);
            self.compressor
                .decompress(&batch.compressed_payload, &mut buf)
                .map_err(PersistError::Decompress)?;

            let metric_batch: MetricBatch =
                self.codec.decode(&buf).map_err(PersistError::Decode)?;
            decoded_batches.push(metric_batch);

            watermarks
                .entry(batch.run_id)
                .and_modify(|s| {
                    if batch.sequence_number > *s {
                        *s = batch.sequence_number;
                    }
                })
                .or_insert(batch.sequence_number);
        }

        // Phase 2: Project (sync, lock held only during CPU work)
        let bucket_entries = {
            let mut downsample = self
                .downsample
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);

            for batch in &decoded_batches {
                downsample.process(batch.run_id, batch);
            }
            downsample.drain()
        };

        // Phase 3: Publish events
        for batch in &decoded_batches {
            let _ = self.event_tx.send(PhotonEvent::BatchDecoded {
                run_id: batch.run_id,
                batch: Arc::new(batch.clone()),
            });
        }
        for (run_id, entries) in &bucket_entries {
            for entry in entries {
                let _ = self.event_tx.send(PhotonEvent::BucketsReduced {
                    run_id: *run_id,
                    key: entry.key.clone(),
                    tier: entry.tier,
                    bucket: entry.bucket.clone(),
                });
            }
        }

        // Phase 4: Flush (parallel across tables)
        let (metrics, watermarks, buckets) = tokio::join!(
            self.flush_metrics(&decoded_batches),
            self.flush_watermarks(watermarks),
            self.flush_buckets(&bucket_entries),
        );
        metrics?;
        watermarks?;
        buckets?;

        Ok(())
    }
}

impl<C, K, M, W, B> Service<C, K, M, W, B>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
{
    async fn flush_metrics(&self, batches: &[MetricBatch]) -> Result<(), PersistError> {
        self.metric_writer
            .write_batches(batches)
            .await
            .map_err(PersistError::MetricWrite)
    }

    async fn flush_watermarks(
        &self,
        watermarks: HashMap<RunId, SequenceNumber>,
    ) -> Result<(), PersistError> {
        let entries: Vec<_> = watermarks.into_iter().collect();
        self.watermark_writer
            .write_watermarks(&entries)
            .await
            .map_err(PersistError::WatermarkWrite)
    }

    async fn flush_buckets(
        &self,
        bucket_entries: &HashMap<RunId, Vec<BucketEntry>>,
    ) -> Result<(), PersistError> {
        for (run_id, entries) in bucket_entries {
            self.bucket_writer
                .write_buckets(run_id, entries)
                .await
                .map_err(PersistError::BucketWrite)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use std::time::SystemTime;

    use photon_core::types::id::RunId;
    use photon_core::types::metric::{Metric, MetricBatch, MetricPoint, Step};
    use photon_core::types::sequence::SequenceNumber;
    use photon_protocol::codec::PostcardCodec;
    use photon_protocol::compressor::NoopCompressor;
    use photon_protocol::ports::codec::Codec;
    use photon_protocol::ports::compress::Compressor;
    use photon_store::memory::bucket::InMemoryBucketStore;
    use photon_store::memory::metric::InMemoryMetricStore;
    use photon_store::memory::watermark::InMemoryWatermarkStore;
    use photon_store::ports::bucket::BucketReader;
    use photon_store::ports::metric::MetricReader;
    use photon_store::ports::watermark::WatermarkReader;

    fn make_wire_batch(batch: &MetricBatch, run_id: RunId, seq: SequenceNumber) -> WireBatch {
        let codec = PostcardCodec;
        let compressor = NoopCompressor;

        let mut encoded = BytesMut::new();
        codec.encode(batch, &mut encoded).expect("encode");

        let uncompressed_size = encoded.len();

        let mut compressed = BytesMut::new();
        compressor
            .compress(&encoded, &mut compressed)
            .expect("compress");

        let crc = crc32fast::hash(&compressed);

        WireBatch {
            run_id,
            sequence_number: seq,
            compressed_payload: compressed.freeze(),
            crc32: crc,
            created_at: SystemTime::now(),
            point_count: batch.points.len(),
            uncompressed_size,
        }
    }

    fn new_service(
        metrics: InMemoryMetricStore,
        watermarks: InMemoryWatermarkStore,
        buckets: InMemoryBucketStore,
    ) -> Service<
        NoopCompressor,
        PostcardCodec,
        InMemoryMetricStore,
        InMemoryWatermarkStore,
        InMemoryBucketStore,
    > {
        let (tx, _) = broadcast::channel(16);
        Service::new(
            NoopCompressor,
            PostcardCodec,
            metrics,
            watermarks,
            buckets,
            tx,
            PersistConfig {
                downsample_widths: vec![5],
                ..PersistConfig::default()
            },
        )
    }

    #[tokio::test]
    async fn test_write_single_batch() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let bucket_store = InMemoryBucketStore::new();
        let svc = new_service(metric_store.clone(), watermark_store.clone(), bucket_store);

        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: vec![
                MetricPoint {
                    key_index: 0,
                    value: 0.5,
                    step: Step::new(1),
                    timestamp_ms: 1000,
                },
                MetricPoint {
                    key_index: 0,
                    value: 0.3,
                    step: Step::new(2),
                    timestamp_ms: 2000,
                },
            ],
        };

        let wire = make_wire_batch(&batch, run_id, SequenceNumber::ZERO);
        svc.write(&[wire]).await.expect("write should succeed");

        let points = metric_store
            .read_points(&run_id, &metric, Step::ZERO..Step::MAX)
            .await
            .expect("read_points");

        assert_eq!(points.len(), 2);
        assert_eq!(points[0], (Step::new(1), 0.5));
        assert_eq!(points[1], (Step::new(2), 0.3));
    }

    #[tokio::test]
    async fn test_write_updates_watermarks() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let bucket_store = InMemoryBucketStore::new();
        let svc = new_service(metric_store, watermark_store.clone(), bucket_store);

        let run_a = RunId::new();
        let run_b = RunId::new();
        let metric = Metric::new("eval/acc").unwrap();

        let batch_a = MetricBatch {
            run_id: run_a,
            keys: vec![metric.clone()],
            points: vec![MetricPoint {
                key_index: 0,
                value: 0.9,
                step: Step::new(1),
                timestamp_ms: 1000,
            }],
        };
        let batch_b = MetricBatch {
            run_id: run_b,
            keys: vec![metric.clone()],
            points: vec![MetricPoint {
                key_index: 0,
                value: 0.8,
                step: Step::new(1),
                timestamp_ms: 2000,
            }],
        };

        let wire_a = make_wire_batch(&batch_a, run_a, SequenceNumber::from(3));
        let wire_b = make_wire_batch(&batch_b, run_b, SequenceNumber::from(7));

        svc.write(&[wire_a, wire_b])
            .await
            .expect("write should succeed");

        let mut watermarks = watermark_store.read_all().await.expect("read_all");
        watermarks.sort_by_key(|(_, seq)| u64::from(*seq));

        assert_eq!(watermarks.len(), 2);

        let (wm_run_a, wm_seq_a) = watermarks[0];
        let (wm_run_b, wm_seq_b) = watermarks[1];

        assert_eq!(wm_run_a, run_a);
        assert_eq!(u64::from(wm_seq_a), 3);

        assert_eq!(wm_run_b, run_b);
        assert_eq!(u64::from(wm_seq_b), 7);
    }

    #[tokio::test]
    async fn test_write_produces_buckets() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let bucket_store = InMemoryBucketStore::new();
        let svc = new_service(metric_store, watermark_store, bucket_store.clone());

        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..11)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        let wire = make_wire_batch(&batch, run_id, SequenceNumber::ZERO);
        svc.write(&[wire]).await.expect("write should succeed");

        let buckets = bucket_store
            .read_buckets(&run_id, &metric, 0, Step::ZERO..Step::MAX)
            .await
            .expect("read_buckets");

        // steps 0-10, width 5 → [0,5) closes at step 5, [5,10) closes at step 10
        assert_eq!(buckets.len(), 2);
    }
}
