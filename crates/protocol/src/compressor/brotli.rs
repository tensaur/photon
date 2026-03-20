use std::io::{Read, Write};

use bytes::BytesMut;

use crate::ports::compress::{CompressionError, Compressor};

/// Excellent compression ratio, slower to compress but fast to decompress.
#[derive(Clone, Debug)]
pub struct BrotliCompressor {
    quality: u32,
}

impl BrotliCompressor {
    pub fn new(quality: u32) -> Self {
        Self { quality }
    }
}

impl Default for BrotliCompressor {
    fn default() -> Self {
        Self::new(4)
    }
}

impl Compressor for BrotliCompressor {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let mut compressed = Vec::new();
        let mut encoder = brotli::CompressorWriter::new(
            &mut compressed,
            4096,
            self.quality,
            22, // lgwin
        );

        encoder
            .write_all(input)
            .map_err(|e| CompressionError::Unknown(e.into()))?;
        drop(encoder);

        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let mut decompressed = Vec::new();
        let mut decoder = brotli::Decompressor::new(input, 4096);

        decoder
            .read_to_end(&mut decompressed)
            .map_err(|_| CompressionError::CorruptPayload {
                compressor_name: self.name().to_owned(),
            })?;

        output.extend_from_slice(&decompressed);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "brotli"
    }
}
