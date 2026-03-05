use std::collections::BTreeMap;

use photon_core::types::batch::AssembledBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::sequence::{SegmentIndex, SequenceNumber};

use crate::domain::ports::wal::{WalError, WalStorage};

/// In-memory WAL for testing
#[derive(Clone)]
pub struct InMemoryWal {
    batches: BTreeMap<SequenceNumber, AssembledBatch>,
    committed: SequenceNumber,
    next_segment: SegmentIndex,
}

impl InMemoryWal {
    pub fn new() -> Self {
        Self {
            batches: BTreeMap::new(),
            committed: SequenceNumber::ZERO,
            next_segment: SegmentIndex::ZERO,
        }
    }
}

impl Default for InMemoryWal {
    fn default() -> Self {
        Self::new()
    }
}

impl WalStorage for InMemoryWal {
    fn append(&mut self, batch: &AssembledBatch) -> Result<(), WalError> {
        self.batches.insert(batch.sequence_number, batch.clone());
        Ok(())
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<SegmentIndex, WalError> {
        let index = self.next_segment;
        self.next_segment = self.next_segment.next();
        Ok(index)
    }

    fn truncate_through(&mut self, sequence: SequenceNumber) -> Result<(), WalError> {
        let keep = self.batches.split_off(&sequence.next());
        self.batches = keep;
        self.committed = sequence;
        Ok(())
    }

    fn read_from(&self, sequence: SequenceNumber) -> Result<Vec<AssembledBatch>, WalError> {
        Ok(self
            .batches
            .range(sequence.next()..)
            .map(|(_, batch)| batch.clone())
            .collect())
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        Ok(WalMeta {
            committed_sequence: self.committed,
        })
    }

    fn delete_all(&mut self) -> Result<(), WalError> {
        self.batches.clear();
        self.committed = SequenceNumber::ZERO;
        Ok(())
    }

    fn total_bytes(&self) -> u64 {
        self.batches
            .values()
            .map(|b| b.compressed_size() as u64)
            .sum()
    }
}
