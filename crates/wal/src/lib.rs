pub mod disk;
pub mod memory;
pub mod ports;

pub use self::disk::{
    DiskWalAppender, DiskWalConfig, DiskWalManager, default_wal_dir, open_disk_wal,
};
pub use self::memory::{InMemoryWalAppender, InMemoryWalManager, open_in_memory_wal};
pub use self::ports::{Wal, WalAppender, WalError};

use std::path::Path;

use photon_core::types::id::RunId;

/// WAL backend selection. Call [`open`](Self::open) to create appender and manager.
#[derive(Clone, Copy, Debug, Default)]
pub enum WalKind {
    #[default]
    Disk,
    Memory,
}

impl WalKind {
    pub fn open(
        self,
        dir: Option<&Path>,
        run_id: RunId,
        config: DiskWalConfig,
    ) -> Result<(Box<dyn WalAppender>, Box<dyn Wal>), WalError> {
        match self {
            Self::Disk => {
                let (a, m) = open_disk_wal(dir, run_id, config)?;
                Ok((Box::new(a), Box::new(m)))
            }
            Self::Memory => {
                let (a, m) = open_in_memory_wal();
                Ok((Box::new(a), Box::new(m)))
            }
        }
    }
}
