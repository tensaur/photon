use bytes::BytesMut;

use crate::ports::compress::{CompressionError, Compressor};

#[derive(Clone, Debug)]
pub struct Lz4Compressor;

impl Lz4Compressor {
    pub const NAME: &str = "lz4";
}

impl Compressor for Lz4Compressor {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let compressed = lz4_flex::compress_prepend_size(input);
        output.extend_from_slice(&compressed);

        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let decompressed = lz4_flex::decompress_size_prepended(input).map_err(|_| {
            CompressionError::CorruptPayload {
                compressor_name: self.name().to_owned(),
            }
        })?;

        output.extend_from_slice(&decompressed);

        Ok(())
    }

    fn name(&self) -> &'static str {
        Self::NAME
    }
}
