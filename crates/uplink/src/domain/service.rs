use std::future::Future;

use photon_core::types::ack::AckResult;
use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use photon_core::types::wal::WalOffset;
use photon_wal::Wal;

use super::ack::{AckTracker, UplinkStats};
use super::error::{RecoveryError, UplinkError};
use super::ports::IngestConnection;

pub trait UplinkService {
    fn recover(&mut self) -> impl Future<Output = Result<SequenceNumber, RecoveryError>> + Send;
    fn send(&mut self, batch: &WireBatch) -> impl Future<Output = Result<(), UplinkError>> + Send;
    fn handle_ack(&mut self, ack: AckResult) -> Result<(), UplinkError>;
    fn sync(&mut self) -> Result<(), UplinkError>;
    fn wal_cursor(&self) -> WalOffset;
    fn stats(&self) -> UplinkStats;
}

pub struct Service<C, M>
where
    C: IngestConnection,
    M: Wal + Clone,
{
    connection: C,
    wal: M,
    run_id: RunId,
    start_sequence: SequenceNumber,
    tracker: AckTracker,
    wal_cursor: WalOffset,
    stats: UplinkStats,
}

impl<C, M> Service<C, M>
where
    C: IngestConnection,
    M: Wal + Clone,
{
    pub fn new(connection: C, wal: M, run_id: RunId, start_sequence: SequenceNumber) -> Self {
        let wal_cursor = wal
            .read_meta()
            .map(|m| m.consumed)
            .unwrap_or(WalOffset::ZERO);
        Self {
            connection,
            wal,
            run_id,
            start_sequence,
            tracker: AckTracker::new(SequenceNumber::ZERO),
            wal_cursor,
            stats: UplinkStats::default(),
        }
    }
}

impl<C, M> UplinkService for Service<C, M>
where
    C: IngestConnection,
    M: Wal + Clone,
{
    async fn recover(&mut self) -> Result<SequenceNumber, RecoveryError> {
        let uncommitted: Vec<_> = self
            .wal
            .read_from(self.wal_cursor)?
            .into_iter()
            .filter(|b| b.sequence_number < self.start_sequence)
            .collect();

        if uncommitted.is_empty() {
            tracing::debug!(run_id = %self.run_id, "clean WAL, skipping recovery");
            return Ok(self.tracker.committed());
        }

        let server = self
            .connection
            .query_watermark(&self.run_id)
            .await
            .map_or(SequenceNumber::ZERO, |seq| seq);

        let local = self.tracker.committed();
        let effective = std::cmp::max(local, server);

        self.tracker = AckTracker::new(effective);

        let replay_batches = self.wal.read_from(self.wal_cursor)?;
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
        self.connection.send_batch(batch).await?;
        self.stats.batches_sent += 1;
        Ok(())
    }

    fn handle_ack(&mut self, ack: AckResult) -> Result<(), UplinkError> {
        let before = self.tracker.committed();
        let outcome = self.tracker.track(ack, &mut self.stats);

        if outcome.new_watermark.is_some() {
            let after = self.tracker.committed();
            let advanced = u64::from(after) - u64::from(before);
            self.wal_cursor = self.wal_cursor.advance(advanced);

            self.wal.truncate_through(self.wal_cursor)?;
            if outcome.should_sync {
                self.wal.sync()?;
            }
        }

        Ok(())
    }

    fn sync(&mut self) -> Result<(), UplinkError> {
        self.wal.sync().map_err(Into::into)
    }

    fn wal_cursor(&self) -> WalOffset {
        self.wal_cursor
    }

    fn stats(&self) -> UplinkStats {
        self.stats.clone()
    }
}
