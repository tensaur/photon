use std::future::Future;

use photon_core::types::ack::AckResult;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_transport::ports::Transport;
use photon_wal::Wal;

use super::ack::{AckTracker, UplinkStats};
use super::error::{RecoveryError, TransportError, UplinkError};

pub trait UplinkService {
    fn recover(&mut self) -> impl Future<Output = Result<SequenceNumber, RecoveryError>> + Send;
    fn send(&mut self, batch: &WireBatch) -> impl Future<Output = Result<(), UplinkError>> + Send;
    fn handle_ack(&mut self, ack: AckResult) -> Result<(), UplinkError>;
    fn sync(&mut self) -> Result<(), UplinkError>;
    fn stats(&self) -> UplinkStats;
}

pub struct Service<T, M>
where
    T: Transport<WireBatch, AckResult>,
    M: Wal + Clone,
{
    transport: T,
    wal: M,
    run_id: RunId,
    tracker: AckTracker,
    stats: UplinkStats,
}

impl<T, M> Service<T, M>
where
    T: Transport<WireBatch, AckResult>,
    M: Wal + Clone,
{
    pub fn new(transport: T, wal: M, run_id: RunId) -> Self {
        Self {
            transport,
            wal,
            run_id,
            tracker: AckTracker::new(SequenceNumber::ZERO),
            stats: UplinkStats::default(),
        }
    }
}

impl<T, M> UplinkService for Service<T, M>
where
    T: Transport<WireBatch, AckResult> + Transport<RunId, SequenceNumber>,
    M: Wal + Clone,
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

    async fn send(&mut self, batch: &WireBatch) -> Result<(), UplinkError> {
        self.transport
            .send(batch)
            .await
            .map_err(TransportError::from)?;
        self.stats.batches_sent += 1;
        Ok(())
    }

    fn handle_ack(&mut self, ack: AckResult) -> Result<(), UplinkError> {
        let outcome = self.tracker.track(ack, &mut self.stats);

        if let Some(watermark) = outcome.new_watermark {
            self.wal.truncate_through(watermark)?;
            if outcome.should_sync {
                self.wal.sync()?;
            }
        }

        Ok(())
    }

    fn sync(&mut self) -> Result<(), UplinkError> {
        self.wal.sync().map_err(Into::into)
    }

    fn stats(&self) -> UplinkStats {
        self.stats.clone()
    }
}
