use bytes::BytesMut;

pub trait Compressor: Send + Sync + Clone + 'static {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError>;

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError>;

    /// Stable identifier written to WAL record headers.
    /// Must not change across versions for a given algorithm.
    fn name(&self) -> &'static str;
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("output buffer too small (needed {needed}, available {available})")]
    BufferTooSmall { needed: usize, available: usize },

    #[error("corrupt payload for compressor {compressor_name:?}")]
    CorruptPayload { compressor_name: String },

    #[error("internal error: {0}")]
    Internal(String),
}
