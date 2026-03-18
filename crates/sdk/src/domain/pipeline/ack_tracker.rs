use std::collections::BTreeSet;

use photon_core::types::sequence::SequenceNumber;

use crate::domain::ports::wal::{WalError, WalManager};

pub struct AckTracker<W: WalManager> {
    wal: W,
    committed: SequenceNumber,
    pending: BTreeSet<SequenceNumber>,
    batches_since_flush: u32,
    flush_interval: u32,
    stats: AckTrackerStats,
}

#[derive(Clone, Debug, Default)]
pub struct AckTrackerStats {
    pub total_acked: u64,
    pub watermark_advances: u64,
    pub segments_truncated: u64,
}

impl<W: WalManager> AckTracker<W> {
    pub fn new(wal: W, committed: SequenceNumber, flush_interval: u32) -> Self {
        Self {
            wal,
            committed,
            pending: BTreeSet::new(),
            batches_since_flush: 0,
            flush_interval,
            stats: AckTrackerStats::default(),
        }
    }

    /// Record an acked sequence number. Advances the watermark through
    /// any contiguous run, then truncates the WAL.
    ///
    /// Example: committed=5, pending={7,8}, on_ack(6) → committed
    /// advances to 8, pending becomes empty, WAL truncates through 8.
    pub fn on_ack(&mut self, seq: SequenceNumber) -> Result<(), WalError> {
        self.pending.insert(seq);
        self.stats.total_acked += 1;

        let before = self.committed;
        self.advance_watermark();

        if self.committed > before {
            self.stats.watermark_advances += 1;
            self.wal.truncate_through(self.committed)?;
            self.stats.segments_truncated += 1;

            self.batches_since_flush += 1;
            if self.batches_since_flush >= self.flush_interval {
                self.flush_meta()?;
                self.batches_since_flush = 0;
            }
        }

        Ok(())
    }

    fn advance_watermark(&mut self) {
        loop {
            let next = self.committed.next();

            if self.pending.remove(&next) {
                self.committed = next;
            } else {
                break;
            }
        }
    }

    /// Persist the current watermark to WAL metadata. Called periodically
    /// and during shutdown to minimise replay on recovery.
    pub fn flush_meta(&self) -> Result<(), WalError> {
        self.wal.sync()
    }

    pub fn shutdown(self) -> Result<AckTrackerStats, WalError> {
        self.flush_meta()?;
        Ok(self.stats)
    }
}
