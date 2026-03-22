use std::fmt;

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::sync::Mutex;

    use bytes::BytesMut;
    use zstd::bulk::{Compressor as Encoder, Decompressor as Decoder};

    use crate::ports::compress::{CompressionError, Compressor};

    pub struct ZstdCompressor {
        pub level: i32,
        compressor: Mutex<Encoder<'static>>,
        decompressor: Mutex<Decoder<'static>>,
    }

    impl ZstdCompressor {
        pub const NAME: &str = "zstd";

        pub fn new(level: i32) -> Self {
            Self {
                level,
                compressor: Mutex::new(
                    Encoder::new(level).expect("failed to create zstd compressor"),
                ),
                decompressor: Mutex::new(
                    Decoder::new().expect("failed to create zstd decompressor"),
                ),
            }
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
            Self::NAME
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use std::io::Read;

    use bytes::BytesMut;
    use ruzstd::decoding::StreamingDecoder;
    use ruzstd::encoding::{CompressionLevel, compress_to_vec};

    use crate::ports::compress::{CompressionError, Compressor};

    pub struct ZstdCompressor {
        pub level: i32,
    }

    impl ZstdCompressor {
        pub const NAME: &str = "zstd";

        pub fn new(level: i32) -> Self {
            Self { level }
        }
    }

    impl Compressor for ZstdCompressor {
        fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
            let level = match self.level {
                0 => CompressionLevel::Uncompressed,
                1..=3 => CompressionLevel::Fastest,
                4..=6 => CompressionLevel::Default,
                7..=9 => CompressionLevel::Better,
                _ => CompressionLevel::Best,
            };

            output.extend_from_slice(&compress_to_vec(input, level));
            Ok(())
        }

        fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
            let mut decoder = StreamingDecoder::new(input)
                .map_err(|e| CompressionError::Unknown(anyhow::anyhow!("zstd frame init: {e}")))?;

            let mut buf = Vec::new();
            decoder
                .read_to_end(&mut buf)
                .map_err(|e| CompressionError::Unknown(anyhow::anyhow!("zstd decompress: {e}")))?;

            output.extend_from_slice(&buf);
            Ok(())
        }

        fn name(&self) -> &'static str {
            Self::NAME
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::ZstdCompressor;
#[cfg(target_arch = "wasm32")]
pub use wasm::ZstdCompressor;

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
