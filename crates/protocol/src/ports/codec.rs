use bytes::BytesMut;

use photon_core::types::metric::MetricBatch;

pub trait Codec<T>: Send + Sync + Clone + 'static {
    fn encode(&self, batch: &T, output: &mut BytesMut) -> Result<(), CodecError>;
    fn decode(&self, input: &[u8]) -> Result<T, CodecError>;
}

pub trait BatchCodec: Send + Sync + Clone + 'static {
    fn encode(&self, batch: &MetricBatch, output: &mut BytesMut) -> Result<(), CodecError>;
    fn decode(&self, input: &[u8]) -> Result<MetricBatch, CodecError>;
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("failed to encode batch: {reason}")]
    EncodeFailed { reason: String },

    #[error("failed to decode batch: {reason}")]
    DecodeFailed { reason: String },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
