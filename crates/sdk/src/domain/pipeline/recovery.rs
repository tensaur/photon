use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::Transport;

use crate::domain::ports::error::TransportError;
use crate::domain::ports::wal::{WalError, WalManager};

pub struct RecoveryManager<W: WalManager> {
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

impl<W: WalManager> RecoveryManager<W> {
    pub fn new(wal: W, run_id: RunId) -> Self {
        Self { wal, run_id }
    }

    pub async fn recover<T>(&self, transport: &T) -> Result<RecoveryResult, RecoveryError>
    where
        T: Transport<RunId, SequenceNumber>,
    {
        let local = self.wal.read_meta()?.committed_sequence;

        transport
            .send(&self.run_id)
            .await
            .map_err(TransportError::from)?;
        let server = transport.recv().await.map_err(TransportError::from)?;

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

    pub fn is_clean(&self) -> Result<bool, WalError> {
        let meta = self.wal.read_meta()?;
        let batches = self.wal.read_from(meta.committed_sequence)?;
        Ok(batches.is_empty())
    }
}
