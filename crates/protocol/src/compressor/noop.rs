use bytes::BytesMut;

use crate::ports::compress::{CompressionError, Compressor};

#[derive(Clone, Debug)]
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
