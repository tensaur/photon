use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::metric::MetricBatch;
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

    /// Decompress, decode, and write WAL entries to the metric store.
    pub async fn write(&self, batches: &[WireBatch]) -> Result<(), FlushError> {
        let mut decoded_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let mut buf = BytesMut::with_capacity(batch.uncompressed_size);
            self.compressor
                .decompress(&batch.compressed_payload, &mut buf)
                .map_err(FlushError::Decompress)?;

            let metric_batch: MetricBatch = self.codec.decode(&buf).map_err(FlushError::Decode)?;
            decoded_batches.push(metric_batch);
        }

        self.metric_writer
            .write_batches(&decoded_batches)
            .await
            .map_err(FlushError::MetricWrite)?;

        Ok(())
    }
}
