use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::sequence::SequenceNumber;

use crate::domain::ports::wal::{WalAppender, WalError, WalManager};

type SharedBatches = Arc<Mutex<BTreeMap<SequenceNumber, WireBatch>>>;

/// In-memory WAL appender for testing.
pub struct InMemoryWalAppender {
    batches: SharedBatches,
}

/// In-memory WAL manager for testing.
#[derive(Clone)]
pub struct InMemoryWalManager {
    batches: SharedBatches,
    committed: SequenceNumber,
}

pub fn open_in_memory_wal() -> (InMemoryWalAppender, InMemoryWalManager) {
    let batches: SharedBatches = Arc::new(Mutex::new(BTreeMap::new()));
    (
        InMemoryWalAppender {
            batches: Arc::clone(&batches),
        },
        InMemoryWalManager {
            batches,
            committed: SequenceNumber::ZERO,
        },
    )
}

impl WalAppender for InMemoryWalAppender {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        self.batches
            .lock()
            .unwrap()
            .insert(batch.sequence_number, batch.clone());
        Ok(())
    }
}

impl WalManager for InMemoryWalManager {
    fn truncate_through(&mut self, sequence: SequenceNumber) -> Result<(), WalError> {
        let mut batches = self.batches.lock().unwrap();
        let keep = batches.split_off(&sequence.next());
        *batches = keep;
        self.committed = sequence;
        Ok(())
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn read_from(&self, sequence: SequenceNumber) -> Result<Vec<WireBatch>, WalError> {
        Ok(self
            .batches
            .lock()
            .unwrap()
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
        self.batches.lock().unwrap().clear();
        self.committed = SequenceNumber::ZERO;
        Ok(())
    }
}
