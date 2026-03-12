pub mod lz4;
pub mod noop;
pub mod zstd;

use bytes::BytesMut;
use crate::ports::compress::{CompressionError, Compressor};
use self::noop::NoopCompressor;
use self::zstd::ZstdCompressor;

#[derive(Clone, Debug)]
pub enum CompressorChoice {
    Zstd(ZstdCompressor),
    Noop(NoopCompressor),
}

impl Default for CompressorChoice {
    fn default() -> Self {
        Self::Zstd(ZstdCompressor::default())
    }
}

impl CompressorChoice {
    pub fn zstd() -> Self {
        Self::default()
    }

    pub fn zstd_with_level(level: i32) -> Self {
        Self::Zstd(ZstdCompressor::new(level))
    }

    pub fn noop() -> Self {
        Self::Noop(NoopCompressor)
    }
}

impl Compressor for CompressorChoice {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        match self {
            Self::Zstd(inner) => inner.compress(input, output),
            Self::Noop(inner) => inner.compress(input, output),
        }
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        match self {
            Self::Zstd(inner) => inner.decompress(input, output),
            Self::Noop(inner) => inner.decompress(input, output),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Zstd(inner) => inner.name(),
            Self::Noop(inner) => inner.name(),
        }
    }
}
