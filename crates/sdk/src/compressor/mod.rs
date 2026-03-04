mod lz4;
mod zstd;

use bytes::BytesMut;
use photon_core::ports::compress::{CompressionError, Compressor};

pub struct NoopCompressor;

impl Compressor for NoopCompressor {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "none"
    }
}
