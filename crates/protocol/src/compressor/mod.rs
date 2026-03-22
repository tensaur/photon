pub mod brotli;
pub mod lz4;
pub mod noop;
pub mod zstd;

pub use self::brotli::BrotliCompressor;
pub use self::lz4::Lz4Compressor;
pub use self::noop::NoopCompressor;
pub use self::zstd::ZstdCompressor;

use bytes::BytesMut;

use crate::ports::compress::{CompressionError, Compressor};

#[derive(Clone, Copy, Debug)]
pub enum CompressorKind {
    Zstd { level: i32 },
    Lz4,
    Brotli { quality: u32 },
    Noop,
}

impl Default for CompressorKind {
    fn default() -> Self {
        Self::Zstd { level: 3 }
    }
}

impl Compressor for CompressorKind {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        match self {
            Self::Zstd { level } => ZstdCompressor::new(*level).compress(input, output),
            Self::Lz4 => Lz4Compressor.compress(input, output),
            Self::Brotli { quality } => BrotliCompressor::new(*quality).compress(input, output),
            Self::Noop => NoopCompressor.compress(input, output),
        }
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        match self {
            Self::Zstd { level } => ZstdCompressor::new(*level).decompress(input, output),
            Self::Lz4 => Lz4Compressor.decompress(input, output),
            Self::Brotli { quality } => BrotliCompressor::new(*quality).decompress(input, output),
            Self::Noop => NoopCompressor.decompress(input, output),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Zstd { .. } => ZstdCompressor::NAME,
            Self::Lz4 => Lz4Compressor::NAME,
            Self::Brotli { .. } => BrotliCompressor::NAME,
            Self::Noop => NoopCompressor::NAME,
        }
    }
}
