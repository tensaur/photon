use std::future::Future;

use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::metric::MetricBatch;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::ports::metric::MetricWriter;

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("decompression failed")]
    Decompress(#[source] photon_protocol::ports::compress::CompressionError),

    #[error("decode failed")]
    Decode(#[source] photon_protocol::ports::codec::CodecError),

    #[error("metric write failed")]
    MetricWrite(#[source] photon_store::ports::WriteError),
}

pub trait PersistService: Send + Sync + 'static {
    fn write(
        &self,
        batches: &[WireBatch],
    ) -> impl Future<Output = Result<(), PersistError>> + Send;
}

pub struct Service<C, K, M>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    compressor: C,
    codec: K,
    metric_writer: M,
}

impl<C, K, M> Service<C, K, M>
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
}

impl<C, K, M> PersistService for Service<C, K, M>
where
    C: Compressor,
    K: Codec<MetricBatch>,
    M: MetricWriter,
{
    async fn write(&self, batches: &[WireBatch]) -> Result<(), PersistError> {
        let mut decoded_batches = Vec::with_capacity(batches.len());

        for batch in batches {
            let mut buf = BytesMut::with_capacity(batch.uncompressed_size);
            self.compressor
                .decompress(&batch.compressed_payload, &mut buf)
                .map_err(PersistError::Decompress)?;

            let metric_batch: MetricBatch = self.codec.decode(&buf).map_err(PersistError::Decode)?;
            decoded_batches.push(metric_batch);
        }

        self.metric_writer
            .write_batches(&decoded_batches)
            .await
            .map_err(PersistError::MetricWrite)?;

        Ok(())
    }
}
