use std::future::Future;

use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::bucket::BucketWriter;
use photon_store::ports::finalised::FinalisedStore;
use photon_store::ports::metric::MetricWriter;
use photon_store::ports::watermark::WatermarkWriter;

use super::changeset::ChangeSet;
use super::projections::Projection;
use super::projections::downsample::{DownsampleConfig, DownsampleProjection};

#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    #[error("metric write failed")]
    MetricWrite(#[source] photon_store::ports::WriteError),

    #[error("watermark write failed")]
    WatermarkWrite(#[source] photon_store::ports::WriteError),

    #[error("bucket write failed")]
    BucketWrite(#[source] photon_store::ports::WriteError),

    #[error("finalised write failed")]
    FinalisedWrite(#[source] photon_store::ports::WriteError),
}

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("decompression failed")]
    Decompress(#[source] photon_protocol::ports::compress::CompressionError),

    #[error("decode failed")]
    Decode(#[source] photon_protocol::ports::codec::CodecError),

    #[error(transparent)]
    Flush(#[from] FlushError),
}

pub trait PersistService: Send + Sync + 'static {
    /// Decode and project wire batches into the changeset
    fn write(
        &mut self,
        batches: &[WireBatch],
        changeset: &mut ChangeSet,
    ) -> Result<(), PersistError>;

    /// Flush partial projection state for a finished run into the changeset
    fn finish_run(&mut self, run_id: RunId, changeset: &mut ChangeSet);

    /// Write the changeset to stores and publish events
    fn flush(
        &self,
        changeset: &mut ChangeSet,
    ) -> impl Future<Output = Result<(), FlushError>> + Send;
}

pub struct Service<C, K, M, W, B, F>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
    F: FinalisedStore,
{
    compressor: C,
    codec: K,
    downsample: DownsampleProjection,
    metric_writer: M,
    watermark_writer: W,
    bucket_writer: B,
    finalised_store: F,
    event_tx: tokio::sync::broadcast::Sender<PhotonEvent>,
}

impl<C, K, M, W, B, F> Service<C, K, M, W, B, F>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
    F: FinalisedStore,
{
    pub fn new(
        compressor: C,
        codec: K,
        metric_writer: M,
        watermark_writer: W,
        bucket_writer: B,
        finalised_store: F,
        event_tx: tokio::sync::broadcast::Sender<photon_core::types::event::PhotonEvent>,
        downsample_config: DownsampleConfig,
    ) -> Self {
        Self {
            compressor,
            codec,
            downsample: DownsampleProjection::new(downsample_config),
            metric_writer,
            watermark_writer,
            bucket_writer,
            finalised_store,
            event_tx,
        }
    }
}

impl<C, K, M, W, B, F> PersistService for Service<C, K, M, W, B, F>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
    B: BucketWriter,
    F: FinalisedStore,
{
    fn write(
        &mut self,
        batches: &[WireBatch],
        changeset: &mut ChangeSet,
    ) -> Result<(), PersistError> {
        for batch in batches {
            let mut buf = BytesMut::with_capacity(batch.uncompressed_size);
            self.compressor
                .decompress(&batch.compressed_payload, &mut buf)
                .map_err(PersistError::Decompress)?;

            let metric_batch: MetricBatch =
                self.codec.decode(&buf).map_err(PersistError::Decode)?;

            self.downsample
                .project(batch.run_id, &metric_batch, changeset);
            changeset.add_decoded_batch(batch.run_id, metric_batch);
            changeset.add_watermark(batch.run_id, batch.sequence_number);
        }

        Ok(())
    }

    fn finish_run(&mut self, run_id: RunId, changeset: &mut ChangeSet) {
        self.downsample.finish_run(run_id, changeset);
        changeset.mark_finalised(run_id);
    }

    async fn flush(&self, changeset: &mut ChangeSet) -> Result<(), FlushError> {
        let watermarks: Vec<_> = changeset
            .watermarks
            .iter()
            .map(|(run_id, seq)| (*run_id, *seq))
            .collect();

        let (metrics_res, watermarks_res, buckets_res, finalised_res) = tokio::join!(
            self.metric_writer.write_batches(&changeset.decoded_batches),
            self.watermark_writer.write_watermarks(&watermarks),
            self.bucket_writer.write_buckets(&changeset.bucket_entries),
            self.finalised_store.mark_finalised_many(&changeset.finalised_runs),
        );
        metrics_res.map_err(FlushError::MetricWrite)?;
        watermarks_res.map_err(FlushError::WatermarkWrite)?;
        buckets_res.map_err(FlushError::BucketWrite)?;
        finalised_res.map_err(FlushError::FinalisedWrite)?;

        for event in changeset.events.drain(..) {
            let _ = self.event_tx.send(event);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use std::time::SystemTime;

    use photon_core::types::event::PhotonEvent;
    use photon_core::types::id::RunId;
    use photon_core::types::metric::{Metric, MetricBatch, MetricPoint, Step};
    use photon_core::types::sequence::SequenceNumber;
    use photon_protocol::codec::PostcardCodec;
    use photon_protocol::compressor::NoopCompressor;
    use photon_protocol::ports::codec::Codec;
    use photon_protocol::ports::compress::Compressor;
    use photon_store::memory::bucket::InMemoryBucketStore;
    use photon_store::memory::finalised::InMemoryFinalisedStore;
    use photon_store::memory::metric::InMemoryMetricStore;
    use photon_store::memory::watermark::InMemoryWatermarkStore;
    use photon_store::ports::bucket::BucketReader;
    use photon_store::ports::metric::MetricReader;
    use photon_store::ports::watermark::WatermarkReader;
    use tokio::sync::broadcast;

    use crate::domain::projections::downsample::DownsampleConfig;

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
        InMemoryFinalisedStore,
    > {
        let (tx, _) = broadcast::channel(16);
        Service::new(
            NoopCompressor,
            PostcardCodec,
            metrics,
            watermarks,
            buckets,
            InMemoryFinalisedStore::new(),
            tx,
            DownsampleConfig { widths: vec![5] },
        )
    }

    #[tokio::test]
    async fn test_write_single_batch() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let bucket_store = InMemoryBucketStore::new();
        let mut svc = new_service(metric_store.clone(), watermark_store.clone(), bucket_store);

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
        let mut changeset = ChangeSet::new();
        svc.write(&[wire], &mut changeset)
            .expect("write should succeed");
        svc.flush(&mut changeset)
            .await
            .expect("flush should succeed");

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
        let mut svc = new_service(metric_store, watermark_store.clone(), bucket_store);

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

        let mut changeset = ChangeSet::new();
        svc.write(&[wire_a, wire_b], &mut changeset)
            .expect("write should succeed");
        svc.flush(&mut changeset)
            .await
            .expect("flush should succeed");

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
        let mut svc = new_service(metric_store, watermark_store, bucket_store.clone());

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
        let mut changeset = ChangeSet::new();
        svc.write(&[wire], &mut changeset)
            .expect("write should succeed");
        svc.flush(&mut changeset)
            .await
            .expect("flush should succeed");

        let buckets = bucket_store
            .read_buckets(&run_id, &metric, 0, Step::ZERO..Step::MAX)
            .await
            .expect("read_buckets");

        // steps 0-10, width 5 → [0,5) closes at step 5, [5,10) closes at step 10
        assert_eq!(buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_events_published_after_flush() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let bucket_store = InMemoryBucketStore::new();
        let (tx, mut rx) = broadcast::channel(16);

        let mut svc = Service::new(
            NoopCompressor,
            PostcardCodec,
            metric_store,
            watermark_store,
            bucket_store,
            InMemoryFinalisedStore::new(),
            tx,
            DownsampleConfig { widths: vec![5] },
        );

        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric],
            points: vec![MetricPoint {
                key_index: 0,
                value: 1.0,
                step: Step::new(0),
                timestamp_ms: 0,
            }],
        };

        let wire = make_wire_batch(&batch, run_id, SequenceNumber::ZERO);
        let mut changeset = ChangeSet::new();
        svc.write(&[wire], &mut changeset)
            .expect("write should succeed");
        svc.flush(&mut changeset)
            .await
            .expect("flush should succeed");

        let event = rx.try_recv().expect("should have received an event");
        assert!(matches!(event, PhotonEvent::BatchDecoded { .. }));
    }

    #[tokio::test]
    async fn test_finish_run_flushes_tail_bucket() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let bucket_store = InMemoryBucketStore::new();
        let mut svc = new_service(metric_store, watermark_store, bucket_store.clone());

        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..3)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        let wire = make_wire_batch(&batch, run_id, SequenceNumber::ZERO);
        let mut changeset = ChangeSet::new();
        svc.write(&[wire], &mut changeset)
            .expect("write should succeed");
        svc.finish_run(run_id, &mut changeset);
        svc.flush(&mut changeset)
            .await
            .expect("flush should succeed");

        let buckets = bucket_store
            .read_buckets(&run_id, &metric, 0, Step::ZERO..Step::MAX)
            .await
            .expect("read_buckets");

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].step_start, Step::new(0));
        assert_eq!(buckets[0].step_end, Step::new(2));
    }
}
