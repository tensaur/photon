use std::collections::HashMap;
use std::time::Instant;

use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;

#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    #[error("decompression failed")]
    Decompress(#[source] photon_protocol::ports::compress::CompressionError),

    #[error("decode failed")]
    Decode(#[source] photon_protocol::ports::codec::CodecError),

    #[error("metric write failed")]
    MetricWrite(#[source] photon_store::ports::WriteError),
}

pub struct FlushStats {
    pub batches: usize,
    pub points: usize,
    pub decode_ms: u64,
    pub write_ms: u64,
    pub watermarks: HashMap<RunId, SequenceNumber>,
}

pub struct FlushService<C, K, M>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    compressor: C,
    codec: K,
    metric_writer: M,
}

impl<C, K, M> FlushService<C, K, M>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    pub fn new(compressor: C, codec: K, metric_writer: M) -> Self {
        Self {
            compressor,
            codec,
            metric_writer,
        }
    }

    /// Process a batch of WAL entries: decompress, decode, write to store.
    /// Returns watermarks (max sequence per run) for the caller to persist.
    pub async fn process(&self, batches: &[WireBatch]) -> Result<FlushStats, FlushError> {
        let mut decoded_batches = Vec::with_capacity(batches.len());
        let mut watermarks: HashMap<RunId, SequenceNumber> = HashMap::new();
        let mut total_points = 0usize;

        // Decode
        let t_decode = Instant::now();
        for batch in batches {
            let mut buf = BytesMut::with_capacity(batch.uncompressed_size);
            self.compressor
                .decompress(&batch.compressed_payload, &mut buf)
                .map_err(FlushError::Decompress)?;

            let metric_batch: MetricBatch = self.codec.decode(&buf).map_err(FlushError::Decode)?;
            total_points += metric_batch.points.len();

            watermarks
                .entry(batch.run_id)
                .and_modify(|s| {
                    if batch.sequence_number > *s {
                        *s = batch.sequence_number;
                    }
                })
                .or_insert(batch.sequence_number);

            decoded_batches.push(metric_batch);
        }
        let decode_ms = t_decode.elapsed().as_millis() as u64;

        // Write metrics
        let t_write = Instant::now();
        self.metric_writer
            .write_batches(&decoded_batches)
            .await
            .map_err(FlushError::MetricWrite)?;
        let write_ms = t_write.elapsed().as_millis() as u64;

        Ok(FlushStats {
            batches: batches.len(),
            points: total_points,
            decode_ms,
            write_ms,
            watermarks,
        })
    }
}
