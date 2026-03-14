use std::fmt;
use std::sync::Mutex;

use bytes::BytesMut;

use crate::ports::compress::{CompressionError, Compressor};

pub struct ZstdCompressor {
    level: i32,
    compressor: Mutex<zstd::bulk::Compressor<'static>>,
    decompressor: Mutex<zstd::bulk::Decompressor<'static>>,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self {
            level,
            compressor: Mutex::new(
                zstd::bulk::Compressor::new(level).expect("failed to create zstd compressor"),
            ),
            decompressor: Mutex::new(
                zstd::bulk::Decompressor::new().expect("failed to create zstd decompressor"),
            ),
        }
    }
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self::new(3)
    }
}

impl Clone for ZstdCompressor {
    fn clone(&self) -> Self {
        Self::new(self.level)
    }
}

impl fmt::Debug for ZstdCompressor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdCompressor")
            .field("level", &self.level)
            .finish()
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let compressed = self
            .compressor
            .lock()
            .unwrap()
            .compress(input)
            .map_err(|e| CompressionError::Unknown(e.into()))?;

        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let capacity = zstd::zstd_safe::get_frame_content_size(input)
            .ok()
            .flatten()
            .map(|s| s as usize)
            .unwrap_or(input.len() * 10);

        let decompressed = self
            .decompressor
            .lock()
            .unwrap()
            .decompress(input, capacity)
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
