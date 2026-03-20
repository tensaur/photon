pub mod brotli;
pub mod lz4;
pub mod noop;
pub mod zstd;

use self::brotli::BrotliCompressor;
use self::lz4::Lz4Compressor;
use self::noop::NoopCompressor;
use self::zstd::ZstdCompressor;
use crate::ports::compress::{CompressionError, Compressor};
use bytes::BytesMut;

#[derive(Clone, Debug)]
pub enum CompressorChoice {
    Zstd(ZstdCompressor),
    Lz4(Lz4Compressor),
    Brotli(BrotliCompressor),
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

    pub fn lz4() -> Self {
        Self::Lz4(Lz4Compressor)
    }

    pub fn brotli() -> Self {
        Self::Brotli(BrotliCompressor::default())
    }

    pub fn brotli_with_quality(quality: u32) -> Self {
        Self::Brotli(BrotliCompressor::new(quality))
    }

    pub fn noop() -> Self {
        Self::Noop(NoopCompressor)
    }
}

impl Compressor for CompressorChoice {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        match self {
            Self::Zstd(inner) => inner.compress(input, output),
            Self::Lz4(inner) => inner.compress(input, output),
            Self::Brotli(inner) => inner.compress(input, output),
            Self::Noop(inner) => inner.compress(input, output),
        }
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        match self {
            Self::Zstd(inner) => inner.decompress(input, output),
            Self::Lz4(inner) => inner.decompress(input, output),
            Self::Brotli(inner) => inner.decompress(input, output),
            Self::Noop(inner) => inner.decompress(input, output),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Zstd(inner) => inner.name(),
            Self::Lz4(inner) => inner.name(),
            Self::Brotli(inner) => inner.name(),
            Self::Noop(inner) => inner.name(),
        }
    }
}
