pub mod client;
pub mod memory;
pub mod ports;
pub(crate) mod segment;
pub mod server;

pub use self::client::{
    ClientWalAppender, ClientWalConfig, ClientWalManager, default_wal_dir, open_client_wal,
};
pub use self::memory::{InMemoryWalAppender, InMemoryWalManager, open_in_memory_wal};
pub use self::ports::{Wal, WalAppender, WalError};

// Backward-compatible aliases
pub type DiskWalAppender = ClientWalAppender;
pub type DiskWalConfig = ClientWalConfig;
pub type DiskWalManager = ClientWalManager;
pub use self::client::open_client_wal as open_disk_wal;

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
        config: ClientWalConfig,
    ) -> Result<(Box<dyn WalAppender>, Box<dyn Wal>), WalError> {
        match self {
            Self::Disk => {
                let (a, m) = open_client_wal(dir, run_id, config)?;
                Ok((Box::new(a), Box::new(m)))
            }
            Self::Memory => {
                let (a, m) = open_in_memory_wal();
                Ok((Box::new(a), Box::new(m)))
            }
        }
    }
}
