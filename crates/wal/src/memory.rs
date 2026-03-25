use std::sync::{Arc, Mutex};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::wal::WalOffset;

use crate::ports::{Wal, WalAppender, WalError};

/// In-memory WAL appender for testing.
pub struct InMemoryWalAppender {
    batches: Arc<Mutex<Vec<WireBatch>>>,
}

/// In-memory WAL for testing.
#[derive(Clone)]
pub struct InMemoryWalManager {
    batches: Arc<Mutex<Vec<WireBatch>>>,
    cursor: WalOffset,
}

pub fn open_in_memory_wal() -> (InMemoryWalAppender, InMemoryWalManager) {
    let batches = Arc::new(Mutex::new(Vec::new()));
    (
        InMemoryWalAppender {
            batches: Arc::clone(&batches),
        },
        InMemoryWalManager {
            batches,
            cursor: WalOffset::ZERO,
        },
    )
}

impl WalAppender for InMemoryWalAppender {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        self.batches.lock().unwrap().push(batch.clone());
        Ok(())
    }
}

impl Wal for InMemoryWalManager {
    fn close(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn truncate_through(&mut self, offset: WalOffset) -> Result<(), WalError> {
        let cursor_val = u64::from(self.cursor);
        let new_val = u64::from(offset);
        let to_remove = (new_val - cursor_val) as usize;

        let mut batches = self.batches.lock().unwrap();
        let n = to_remove.min(batches.len());
        batches.drain(..n);
        self.cursor = offset;
        Ok(())
    }

    fn sync(&self) -> Result<(), WalError> {
        Ok(())
    }

    fn read_from(&self, offset: WalOffset) -> Result<Vec<WireBatch>, WalError> {
        let cursor_val = u64::from(self.cursor);
        let offset_val = u64::from(offset);
        let skip = (offset_val - cursor_val) as usize;

        let batches = self.batches.lock().unwrap();
        Ok(batches.iter().skip(skip).cloned().collect())
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        Ok(WalMeta {
            cursor: self.cursor,
            consumed: self.cursor,
        })
    }

    fn delete_all(&mut self) -> Result<(), WalError> {
        self.batches.lock().unwrap().clear();
        self.cursor = WalOffset::ZERO;
        Ok(())
    }
}
