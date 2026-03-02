use bytes::BytesMut;

use crate::types::metric::MetricBatch;

pub trait BatchCodec: Send + 'static {
    fn encode(
        &self,
        batch: &MetricBatch,
        output: &mut BytesMut,
    ) -> Result<(), CodecError>;

    fn decode(
        &self,
        input: &[u8],
    ) -> Result<MetricBatch, CodecError>;
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
