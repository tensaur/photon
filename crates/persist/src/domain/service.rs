use std::collections::HashMap;
use std::future::Future;

use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;
use photon_store::ports::watermark::WatermarkWriter;

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
}

pub trait PersistService: Clone + Send + Sync + 'static {
    fn write(&self, batches: &[WireBatch])
    -> impl Future<Output = Result<(), PersistError>> + Send;
}

#[derive(Clone)]
pub struct Service<C, K, M, W>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
{
    compressor: C,
    codec: K,
    metric_writer: M,
    watermark_writer: W,
}

impl<C, K, M, W> Service<C, K, M, W>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
{
    pub fn new(compressor: C, codec: K, metric_writer: M, watermark_writer: W) -> Self {
        Self {
            compressor,
            codec,
            metric_writer,
            watermark_writer,
        }
    }
}

impl<C, K, M, W> PersistService for Service<C, K, M, W>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkWriter,
{
    async fn write(&self, batches: &[WireBatch]) -> Result<(), PersistError> {
        let mut decoded_batches = Vec::with_capacity(batches.len());
        let mut watermarks: HashMap<RunId, SequenceNumber> = HashMap::new();

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

        self.metric_writer
            .write_batches(&decoded_batches)
            .await
            .map_err(PersistError::MetricWrite)?;

        let entries: Vec<_> = watermarks.into_iter().collect();
        self.watermark_writer
            .write_watermarks(&entries)
            .await
            .map_err(PersistError::WatermarkWrite)?;

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
    use photon_store::memory::metric::InMemoryMetricStore;
    use photon_store::memory::watermark::InMemoryWatermarkStore;
    use photon_store::ports::metric::MetricReader;
    use photon_store::ports::watermark::WatermarkReader;

    /// Encode and compress a MetricBatch into a WireBatch (the inverse of what
    /// the persist service does on the read path).
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
    ) -> Service<NoopCompressor, PostcardCodec, InMemoryMetricStore, InMemoryWatermarkStore> {
        Service::new(NoopCompressor, PostcardCodec, metrics, watermarks)
    }

    #[tokio::test]
    async fn test_write_single_batch() {
        let metric_store = InMemoryMetricStore::new();
        let watermark_store = InMemoryWatermarkStore::new();
        let svc = new_service(metric_store.clone(), watermark_store.clone());

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
        let svc = new_service(metric_store, watermark_store);

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

        // Read from the service's owned watermark writer.
        let mut watermarks = svc.watermark_writer.read_all().await.expect("read_all");
        watermarks.sort_by_key(|(_, seq)| u64::from(*seq));

        assert_eq!(watermarks.len(), 2);

        let (wm_run_a, wm_seq_a) = watermarks[0];
        let (wm_run_b, wm_seq_b) = watermarks[1];

        assert_eq!(wm_run_a, run_a);
        assert_eq!(u64::from(wm_seq_a), 3);

        assert_eq!(wm_run_b, run_b);
        assert_eq!(u64::from(wm_seq_b), 7);
    }
}
