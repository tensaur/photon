use std::collections::BTreeSet;

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::sequence::SequenceNumber;

#[derive(Clone, Debug, Default)]
pub struct UplinkStats {
    pub batches_sent: u64,
    pub batches_acked: u64,
    pub batches_retried: u64,
    pub duplicates_received: u64,
    pub rejections_received: u64,
    pub watermark_advances: u64,
    pub segments_truncated: u64,
}

pub struct AckOutcome {
    pub new_watermark: Option<SequenceNumber>,
    pub should_sync: bool,
}

pub struct AckTracker {
    committed: SequenceNumber,
    pending: BTreeSet<SequenceNumber>,
    batches_since_sync: u32,
    sync_interval: u32,
}

impl AckTracker {
    pub fn new(committed: SequenceNumber) -> Self {
        Self {
            committed,
            pending: BTreeSet::new(),
            batches_since_sync: 0,
            sync_interval: 10,
        }
    }

    pub fn committed(&self) -> SequenceNumber {
        self.committed
    }

    pub fn track(&mut self, ack: &AckResult, stats: &mut UplinkStats) -> AckOutcome {
        let seq = ack.sequence_number;

        match ack.status {
            AckStatus::Ok => stats.batches_acked += 1,
            AckStatus::Duplicate => {
                stats.batches_acked += 1;
                stats.duplicates_received += 1;
            }
            AckStatus::Rejected => {
                stats.rejections_received += 1;
                tracing::warn!(
                    sequence = u64::from(seq),
                    "batch permanently rejected by server, advancing past it"
                );
            }
        }

        self.pending.insert(seq);

        let before = self.committed;
        loop {
            let next = self.committed.next();
            if self.pending.remove(&next) {
                self.committed = next;
            } else {
                break;
            }
        }

        if self.committed > before {
            stats.watermark_advances += 1;
            stats.segments_truncated += 1;

            self.batches_since_sync += 1;
            let should_sync = self.batches_since_sync >= self.sync_interval;
            if should_sync {
                self.batches_since_sync = 0;
            }

            AckOutcome {
                new_watermark: Some(self.committed),
                should_sync,
            }
        } else {
            AckOutcome {
                new_watermark: None,
                should_sync: false,
            }
        }
    }
}
