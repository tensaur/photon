pub mod disk;
pub mod memory;
pub mod ports;

pub use self::disk::{
    DiskWalAppender, DiskWalConfig, DiskWalManager, default_wal_dir, open_disk_wal,
};
pub use self::memory::{InMemoryWalAppender, InMemoryWalManager, open_in_memory_wal};
pub use self::ports::{Wal, WalAppender, WalError};

use std::path::PathBuf;
use std::sync::Arc;

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
        dir: impl Into<PathBuf>,
        config: DiskWalConfig,
    ) -> Result<(Box<dyn WalAppender>, Arc<dyn Wal>), WalError> {
        match self {
            Self::Disk => {
                let (a, m) = open_disk_wal(dir, config)?;
                Ok((Box::new(a), Arc::new(m)))
            }
            Self::Memory => {
                let (a, m) = open_in_memory_wal();
                Ok((Box::new(a), Arc::new(m)))
            }
        }
    }
}
