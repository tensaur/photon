use photon_core::types::batch::WireBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::sequence::{SegmentIndex, SequenceNumber};

/// Abstraction over the write-ahead log storage backend.
/// The WAL is the durability boundary of the pipeline.
pub trait WalStorage: Send + Sync + Clone + 'static {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError>;

    fn sync(&self) -> Result<(), WalError>;

    fn rotate_segment(&mut self) -> Result<SegmentIndex, WalError>;

    /// Delete all segments whose last sequence leq the given watermark.
    fn truncate_through(&mut self, sequence: SequenceNumber) -> Result<(), WalError>;

    /// Read all batches with sequence gt the given watermark.
    fn read_from(&self, sequence: SequenceNumber) -> Result<Vec<WireBatch>, WalError>;

    /// Read the first batch with sequence gt the given watermark.
    fn read_next(&self, after: SequenceNumber) -> Result<Option<WireBatch>, WalError> {
        Ok(self.read_from(after)?.into_iter().next())
    }

    fn read_meta(&self) -> Result<WalMeta, WalError>;

    fn delete_all(&mut self) -> Result<(), WalError>;

    fn total_bytes(&self) -> u64;
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("corrupt segment {index} at byte offset {offset}")]
    CorruptSegment { index: SegmentIndex, offset: u64 },

    #[error("WAL disk budget exceeded ({used} / {budget} bytes)")]
    DiskFull { budget: u64, used: u64 },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
