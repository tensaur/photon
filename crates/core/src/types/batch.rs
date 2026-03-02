use bytes::Bytes;
use std::time::SystemTime;

use crate::types::{id::RunId, sequence::SequenceNumber};

/// A compressed, wire-ready batch of metric points.
#[derive(Clone, Debug)]
pub struct AssembledBatch {
    pub run_id: RunId,
    pub sequence_number: SequenceNumber,
    pub compressed_payload: Bytes,
    pub crc32: u32,
    pub created_at: SystemTime,
    pub point_count: usize,
    pub uncompressed_size: usize,
}

impl AssembledBatch {
    pub fn compressed_size(&self) -> usize {
        self.compressed_payload.len()
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.compressed_payload.is_empty() {
            return 0.0;
        }

        self.uncompressed_size as f64 / self.compressed_payload.len() as f64
    }
}
