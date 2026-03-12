mod disk;
mod memory;
mod segment;

pub(crate) use self::disk::{DiskWalConfig, SharedDiskWal};
pub(crate) use self::memory::InMemoryWal;

use photon_core::types::batch::AssembledBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::id::RunId;
use photon_core::types::sequence::{SegmentIndex, SequenceNumber};

use crate::domain::ports::wal::{WalError, WalStorage};

#[derive(Clone)]
pub enum WalChoice {
    Disk(SharedDiskWal),
    Memory(InMemoryWal),
}

impl WalChoice {
    pub(crate) fn open(
        wal_dir: Option<&std::path::Path>,
        run_id: RunId,
        in_memory: bool,
    ) -> Result<Self, WalError> {
        if in_memory {
            Ok(Self::Memory(InMemoryWal::new()))
        } else {
            SharedDiskWal::open(wal_dir, run_id, DiskWalConfig::default()).map(Self::Disk)
        }
    }
}

impl WalStorage for WalChoice {
    fn append(&mut self, batch: &AssembledBatch) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.append(batch),
            Self::Memory(inner) => inner.append(batch),
        }
    }

    fn sync(&self) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.sync(),
            Self::Memory(inner) => inner.sync(),
        }
    }

    fn rotate_segment(&mut self) -> Result<SegmentIndex, WalError> {
        match self {
            Self::Disk(inner) => inner.rotate_segment(),
            Self::Memory(inner) => inner.rotate_segment(),
        }
    }

    fn truncate_through(&mut self, sequence: SequenceNumber) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.truncate_through(sequence),
            Self::Memory(inner) => inner.truncate_through(sequence),
        }
    }

    fn read_from(&self, sequence: SequenceNumber) -> Result<Vec<AssembledBatch>, WalError> {
        match self {
            Self::Disk(inner) => inner.read_from(sequence),
            Self::Memory(inner) => inner.read_from(sequence),
        }
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        match self {
            Self::Disk(inner) => inner.read_meta(),
            Self::Memory(inner) => inner.read_meta(),
        }
    }

    fn delete_all(&mut self) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.delete_all(),
            Self::Memory(inner) => inner.delete_all(),
        }
    }

    fn total_bytes(&self) -> u64 {
        match self {
            Self::Disk(inner) => inner.total_bytes(),
            Self::Memory(inner) => inner.total_bytes(),
        }
    }
}
