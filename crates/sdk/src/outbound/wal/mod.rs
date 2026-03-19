mod disk;
mod memory;
mod segment;

pub(crate) use self::disk::{DiskWalAppender, DiskWalConfig, DiskWalManager};
pub(crate) use self::memory::{InMemoryWalAppender, InMemoryWalManager};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::domain::ports::wal::{WalAppender, WalError, WalManager};

pub enum WalChoice {
    Disk,
    Memory,
}

impl WalChoice {
    pub(crate) fn open(
        self,
        wal_dir: Option<&std::path::Path>,
        run_id: RunId,
    ) -> Result<(WalAppenderChoice, WalManagerChoice), WalError> {
        match self {
            Self::Memory => {
                let (a, m) = memory::open_in_memory_wal();
                Ok((WalAppenderChoice::Memory(a), WalManagerChoice::Memory(m)))
            }
            Self::Disk => {
                let (a, m) = disk::open_disk_wal(wal_dir, run_id, DiskWalConfig::default())?;
                Ok((WalAppenderChoice::Disk(a), WalManagerChoice::Disk(m)))
            }
        }
    }
}

pub enum WalAppenderChoice {
    Disk(DiskWalAppender),
    Memory(InMemoryWalAppender),
}

#[derive(Clone)]
pub enum WalManagerChoice {
    Disk(DiskWalManager),
    Memory(InMemoryWalManager),
}

impl WalAppender for WalAppenderChoice {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.append(batch),
            Self::Memory(inner) => inner.append(batch),
        }
    }
}

impl WalManager for WalManagerChoice {
    fn truncate_through(&mut self, sequence: SequenceNumber) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.truncate_through(sequence),
            Self::Memory(inner) => inner.truncate_through(sequence),
        }
    }

    fn sync(&self) -> Result<(), WalError> {
        match self {
            Self::Disk(inner) => inner.sync(),
            Self::Memory(inner) => inner.sync(),
        }
    }

    fn read_from(&self, sequence: SequenceNumber) -> Result<Vec<WireBatch>, WalError> {
        match self {
            Self::Disk(inner) => inner.read_from(sequence),
            Self::Memory(inner) => inner.read_from(sequence),
        }
    }

    fn read_next(&self, after: SequenceNumber) -> Result<Option<WireBatch>, WalError> {
        match self {
            Self::Disk(inner) => inner.read_next(after),
            Self::Memory(inner) => inner.read_next(after),
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
