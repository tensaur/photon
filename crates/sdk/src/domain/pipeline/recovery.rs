use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::domain::ports::transport::{BatchTransport, TransportError};
use crate::domain::ports::wal::{WalError, WalStorage};

pub struct RecoveryManager<W: WalStorage> {
    wal: W,
    run_id: RunId,
}

#[derive(Clone, Debug)]
pub struct RecoveryResult {
    pub effective_watermark: SequenceNumber,
    pub batches_to_replay: usize,
    pub bytes_to_replay: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("WAL error during recovery")]
    Wal(#[from] WalError),

    #[error("transport error during recovery")]
    Transport(#[from] TransportError),
}

impl<W: WalStorage> RecoveryManager<W> {
    pub fn new(wal: W, run_id: RunId) -> Self {
        Self { wal, run_id }
    }

    /// Determine the recovery starting point and what needs replaying.
    /// Compares two watermarks:
    /// - Local: persisted in WAL metadata.
    /// - Server: what the server actually commited. Can be ahead of
    ///   local if the SDK crashed after the server acked but before
    ///   the ack tracker flushed.
    pub async fn recover<T: BatchTransport>(
        &self,
        transport: &T,
    ) -> Result<RecoveryResult, RecoveryError> {
        let local = self.wal.read_meta()?.committed_sequence;
        let server = transport.get_watermark(&self.run_id).await?;
        let effective = std::cmp::max(local, server);

        let batches = self.wal.read_from(effective)?;
        let bytes_to_replay = batches.iter().map(|b| b.compressed_size() as u64).sum();

        tracing::info!(
            run_id = %self.run_id,
            local_watermark = u64::from(local),
            server_watermark = u64::from(server),
            effective_watermark = u64::from(effective),
            batches_to_replay = batches.len(),
            bytes_to_replay,
            "recovery assessment complete"
        );

        Ok(RecoveryResult {
            effective_watermark: effective,
            batches_to_replay: batches.len(),
            bytes_to_replay,
        })
    }

    /// Check for clean recovery, i.e. no WAL data exists
    pub fn is_clean(&self) -> Result<bool, WalError> {
        let meta = self.wal.read_meta()?;
        let batches = self.wal.read_from(meta.committed_sequence)?;
        Ok(batches.is_empty())
    }
}
