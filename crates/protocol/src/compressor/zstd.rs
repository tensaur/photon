use bytes::BytesMut;

use crate::ports::compress::{CompressionError, Compressor};

#[derive(Clone)]
pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self { level: 3 }
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let compressed = zstd::encode_all(input, self.level)
            .map_err(|e| CompressionError::Unknown(e.into()))?;

        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let decompressed = zstd::decode_all(input)
            .map_err(|_| CompressionError::CorruptPayload {
                compressor_name: self.name().to_owned(),
            })?;

        output.extend_from_slice(&decompressed);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}
