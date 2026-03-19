use std::future::Future;

use photon_core::types::ack::AckResult;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::Transport;
use photon_wal::WalManager;

use super::ack::{AckTracker, SenderStats};
use super::error::{RecoveryError, SendError, TransportError};

pub trait SenderService {
    fn recover(
        &mut self,
    ) -> impl Future<Output = Result<SequenceNumber, RecoveryError>> + Send;

    fn send(
        &mut self,
        batch: &WireBatch,
    ) -> impl Future<Output = Result<(), SendError>> + Send;

    fn handle_ack(&mut self, ack: AckResult) -> Result<(), SendError>;

    fn sync(&mut self) -> Result<(), SendError>;

    fn stats(&self) -> SenderStats;
}

pub struct Service<T, M>
where
    T: Transport<WireBatch, AckResult>,
    M: WalManager,
{
    transport: T,
    wal: M,
    run_id: RunId,
    tracker: AckTracker,
    stats: SenderStats,
}

impl<T, M> Service<T, M>
where
    T: Transport<WireBatch, AckResult>,
    M: WalManager,
{
    pub fn new(transport: T, wal: M, run_id: RunId) -> Self {
        Self {
            transport,
            wal,
            run_id,
            tracker: AckTracker::new(SequenceNumber::ZERO),
            stats: SenderStats::default(),
        }
    }
}

impl<T, M> SenderService for Service<T, M>
where
    T: Transport<WireBatch, AckResult> + Transport<RunId, SequenceNumber>,
    M: WalManager,
{
    async fn recover(&mut self) -> Result<SequenceNumber, RecoveryError> {
        let meta = self.wal.read_meta()?;
        let uncommitted = self.wal.read_from(meta.committed_sequence)?;

        if uncommitted.is_empty() {
            tracing::debug!(run_id = %self.run_id, "clean WAL, skipping recovery");
            return Ok(self.tracker.committed());
        }

        self.transport
            .send(&self.run_id)
            .await
            .map_err(TransportError::from)?;
        let server: SequenceNumber = <T as Transport<RunId, SequenceNumber>>::recv(&self.transport)
            .await
            .map_err(TransportError::from)?;

        let local = meta.committed_sequence;
        let effective = std::cmp::max(local, server);

        self.tracker = AckTracker::new(effective);

        let replay_batches = self.wal.read_from(effective)?;
        let bytes_to_replay: u64 = replay_batches
            .iter()
            .map(|b| b.compressed_size() as u64)
            .sum();

        if !replay_batches.is_empty() {
            tracing::info!(
                run_id = %self.run_id,
                local_watermark = u64::from(local),
                server_watermark = u64::from(server),
                effective_watermark = u64::from(effective),
                batches = replay_batches.len(),
                bytes = bytes_to_replay,
                "replaying uncommitted batches from WAL"
            );
        }

        Ok(effective)
    }

    async fn send(&mut self, batch: &WireBatch) -> Result<(), SendError> {
        self.transport
            .send(batch)
            .await
            .map_err(TransportError::from)?;
        self.stats.batches_sent += 1;
        Ok(())
    }

    fn handle_ack(&mut self, ack: AckResult) -> Result<(), SendError> {
        let outcome = self.tracker.track(ack, &mut self.stats);

        if let Some(watermark) = outcome.new_watermark {
            self.wal.truncate_through(watermark)?;
            if outcome.should_sync {
                self.wal.sync()?;
            }
        }

        Ok(())
    }

    fn sync(&mut self) -> Result<(), SendError> {
        self.wal.sync().map_err(Into::into)
    }

    fn stats(&self) -> SenderStats {
        self.stats.clone()
    }
}
