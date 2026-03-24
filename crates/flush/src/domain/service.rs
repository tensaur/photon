use std::collections::HashMap;

use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;
use photon_store::ports::watermark::WatermarkStore;

#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    #[error("decompression failed")]
    Decompress(#[source] photon_protocol::ports::compress::CompressionError),

    #[error("decode failed")]
    Decode(#[source] photon_protocol::ports::codec::CodecError),

    #[error("metric write failed")]
    MetricWrite(#[source] photon_store::ports::WriteError),

    #[error("watermark advance failed")]
    WatermarkWrite(#[source] photon_store::ports::WriteError),
}

pub struct FlushService<C, K, M, W>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkStore,
{
    compressor: C,
    codec: K,
    metric_writer: M,
    watermark_store: W,
}

impl<C, K, M, W> FlushService<C, K, M, W>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
    W: WatermarkStore,
{
    pub fn new(compressor: C, codec: K, metric_writer: M, watermark_store: W) -> Self {
        Self {
            compressor,
            codec,
            metric_writer,
            watermark_store,
        }
    }

    /// Process a batch of WAL entries: decompress, decode, write to store.
    pub async fn process(&self, batches: &[WireBatch]) -> Result<(), FlushError> {
        let mut decoded_batches = Vec::with_capacity(batches.len());

        // Track max watermark per run
        let mut watermarks: HashMap<RunId, SequenceNumber> = HashMap::new();

        for batch in batches {
            // Decompress
            let mut buf = BytesMut::with_capacity(batch.uncompressed_size);
            self.compressor
                .decompress(&batch.compressed_payload, &mut buf)
                .map_err(FlushError::Decompress)?;

            // Decode
            let metric_batch: MetricBatch = self.codec.decode(&buf).map_err(FlushError::Decode)?;

            // Track max watermark per run
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

        // Write decoded points in bulk so a single flush cycle can amortize insert overhead.
        self.metric_writer
            .write_batches(&decoded_batches)
            .await
            .map_err(FlushError::MetricWrite)?;

        // Advance watermarks
        let watermark_updates: Vec<_> = watermarks.into_iter().collect();
        self.watermark_store
            .advance_many(&watermark_updates)
            .await
            .map_err(FlushError::WatermarkWrite)?;

        Ok(())
    }
}
