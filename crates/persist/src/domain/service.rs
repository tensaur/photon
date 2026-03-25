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

pub trait PersistService: Send + Sync + 'static {
    fn write(
        &self,
        batches: &[WireBatch],
    ) -> impl Future<Output = Result<(), PersistError>> + Send;
}

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

            let metric_batch: MetricBatch = self.codec.decode(&buf).map_err(PersistError::Decode)?;
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
