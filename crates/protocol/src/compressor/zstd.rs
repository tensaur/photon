use std::cell::RefCell;
use std::fmt;
use std::sync::Mutex;

use bytes::BytesMut;
use zrip::{CompressContext, DecompressContext};

use crate::ports::compress::{CompressionError, Compressor};

/// Reuses zrip contexts across calls. Each context is created on first use,
/// so a compressor that only compresses never allocates decoder state.
pub struct ZstdCompressor {
    pub level: i32,
    compressor: Mutex<Option<CompressContext>>,
    decompressor: Mutex<Option<DecompressContext>>,
}

impl ZstdCompressor {
    pub const NAME: &str = "zstd";

    pub fn new(level: i32) -> Self {
        Self {
            level,
            compressor: Mutex::new(None),
            decompressor: Mutex::new(None),
        }
    }

    /// Compression for `CompressorKind`, which has no place to keep a
    /// context: reuses one per thread, so each call skips allocating and
    /// clearing match tables.
    pub(crate) fn compress_cached(
        level: i32,
        input: &[u8],
        output: &mut BytesMut,
    ) -> Result<(), CompressionError> {
        let internal = |e: zrip::CompressError| CompressionError::Internal(e.to_string());
        KIND_COMPRESSOR.with_borrow_mut(|slot| {
            let level = zrip_level(level);
            let context = match slot {
                Some(context) => {
                    context.set_level(level).map_err(internal)?;
                    context
                }
                None => slot.insert(CompressContext::new(level).map_err(internal)?),
            };
            output.extend_from_slice(&context.compress(input).map_err(internal)?);
            Ok(())
        })
    }

    /// Decompression for `CompressorKind`, reusing one context per thread.
    pub(crate) fn decompress_cached(
        input: &[u8],
        output: &mut BytesMut,
    ) -> Result<(), CompressionError> {
        KIND_DECOMPRESSOR.with_borrow_mut(|slot| {
            decompress_with(
                slot.get_or_insert_with(DecompressContext::new),
                input,
                output,
            )
        })
    }
}

thread_local! {
    static KIND_COMPRESSOR: RefCell<Option<CompressContext>> = const { RefCell::new(None) };
    static KIND_DECOMPRESSOR: RefCell<Option<DecompressContext>> = const { RefCell::new(None) };
}

/// Decompresses one frame, bounded by its declared content size, else 10x
/// the input (the bound C zstd was given before).
fn decompress_with(
    context: &mut DecompressContext,
    input: &[u8],
    output: &mut BytesMut,
) -> Result<(), CompressionError> {
    let limit = zrip::frame::header::parse_frame_header(input)
        .ok()
        .and_then(|header| header.frame_content_size)
        .map_or(input.len().saturating_mul(10), |size| {
            usize::try_from(size).unwrap_or(usize::MAX)
        });
    let decompressed = context.decompress_with_limit(input, limit).map_err(|_| {
        CompressionError::CorruptPayload {
            compressor_name: ZstdCompressor::NAME.to_owned(),
        }
    })?;
    output.extend_from_slice(&decompressed);
    Ok(())
}

/// zrip supports levels -7..=4. Higher levels (C zstd goes up to 22) clamp
/// to 4, zrip's strongest. Level 0 is zrip's default, level 1.
fn zrip_level(level: i32) -> i32 {
    level.clamp(-7, 4)
}

impl Compressor for ZstdCompressor {
    fn compress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let mut context = self.compressor.lock().unwrap();
        if context.is_none() {
            let created = CompressContext::new(zrip_level(self.level))
                .map_err(|e| CompressionError::Internal(e.to_string()))?;
            *context = Some(created);
        }
        let compressed = context
            .as_mut()
            .expect("context created above")
            .compress(input)
            .map_err(|e| CompressionError::Internal(e.to_string()))?;

        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut BytesMut) -> Result<(), CompressionError> {
        let mut context = self.decompressor.lock().unwrap();
        decompress_with(
            context.get_or_insert_with(DecompressContext::new),
            input,
            output,
        )
    }

    fn name(&self) -> &'static str {
        Self::NAME
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

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for ZstdCompressor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdCompressor")
            .field("level", &self.level)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(compressor: &ZstdCompressor, input: &[u8]) -> Vec<u8> {
        let mut compressed = BytesMut::new();
        compressor.compress(input, &mut compressed).unwrap();
        let mut decompressed = BytesMut::new();
        compressor
            .decompress(&compressed, &mut decompressed)
            .unwrap();
        decompressed.to_vec()
    }

    #[test]
    fn round_trips_with_reused_contexts() {
        let compressor = ZstdCompressor::default();
        for size in [0, 1, 1_000, 300_000] {
            let input: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            assert_eq!(round_trip(&compressor, &input), input);
        }
    }

    #[test]
    fn clamps_levels_outside_zrip_range() {
        let input = b"metric batch ".repeat(100);
        for level in [-100, -7, 0, 3, 4, 5, 19, 22] {
            assert_eq!(round_trip(&ZstdCompressor::new(level), &input), input);

            let mut compressed = BytesMut::new();
            ZstdCompressor::compress_cached(level, &input, &mut compressed).unwrap();
            let mut decompressed = BytesMut::new();
            ZstdCompressor::new(level)
                .decompress(&compressed, &mut decompressed)
                .unwrap();
            assert_eq!(&decompressed[..], &input[..]);
        }
    }

    #[test]
    fn appends_to_existing_output() {
        let compressor = ZstdCompressor::default();
        let mut compressed = BytesMut::new();
        compressor.compress(b"payload", &mut compressed).unwrap();
        let mut output = BytesMut::from(&b"prefix:"[..]);
        compressor.decompress(&compressed, &mut output).unwrap();
        assert_eq!(&output[..], b"prefix:payload");
    }

    #[test]
    fn rejects_corrupt_payloads() {
        let compressor = ZstdCompressor::default();
        let mut compressed = BytesMut::new();
        compressor.compress(&[7; 1_000], &mut compressed).unwrap();
        let truncated = &compressed[..compressed.len() - 1];
        let mut output = BytesMut::new();
        assert!(matches!(
            compressor.decompress(truncated, &mut output),
            Err(CompressionError::CorruptPayload { .. })
        ));
        assert!(compressor.decompress(b"not zstd", &mut output).is_err());
    }

    #[test]
    fn bounds_frames_without_content_size_to_10x_input() {
        use std::io::Write;

        let unsized_frame = |input: &[u8]| {
            let mut encoder = zrip::FrameEncoder::new(Vec::new(), 1).unwrap();
            encoder.write_all(input).unwrap();
            let frame = encoder.finish().unwrap();
            let header = zrip::frame::header::parse_frame_header(&frame).unwrap();
            assert_eq!(header.frame_content_size, None);
            frame
        };
        let compressor = ZstdCompressor::default();
        let mut output = BytesMut::new();

        let text = b"step 1 loss 0.25\n".repeat(4);
        compressor
            .decompress(&unsized_frame(&text), &mut output)
            .unwrap();
        assert_eq!(&output[..], &text[..]);

        let zeros = unsized_frame(&vec![0; 100_000]);
        assert!(zeros.len() * 10 < 100_000);
        assert!(compressor.decompress(&zeros, &mut output).is_err());
    }
}
