use dyn_clone::DynClone;
use photon_core::types::batch::WireBatch;
use photon_core::types::config::WalMeta;
use photon_core::types::wal::{SegmentIndex, WalOffset};

/// WAL lifecycle and read/truncate operations.
/// Split from [`WalAppender`] so the append path has exclusive `&mut self` access
/// without contention from the reader/truncator on another thread.
pub trait Wal: DynClone + Send + 'static {
    fn close(&self) -> Result<(), WalError>;

    fn truncate_through(&mut self, offset: WalOffset) -> Result<(), WalError>;

    fn sync(&self) -> Result<(), WalError>;

    fn read_from(&self, offset: WalOffset) -> Result<Vec<WireBatch>, WalError>;

    fn read_meta(&self) -> Result<WalMeta, WalError>;

    fn delete_all(&mut self) -> Result<(), WalError>;
}

dyn_clone::clone_trait_object!(Wal);

pub trait WalAppender: Send + 'static {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError>;
}

impl Wal for Box<dyn Wal> {
    fn close(&self) -> Result<(), WalError> {
        (**self).close()
    }

    fn truncate_through(&mut self, offset: WalOffset) -> Result<(), WalError> {
        (**self).truncate_through(offset)
    }

    fn sync(&self) -> Result<(), WalError> {
        (**self).sync()
    }

    fn read_from(&self, offset: WalOffset) -> Result<Vec<WireBatch>, WalError> {
        (**self).read_from(offset)
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        (**self).read_meta()
    }

    fn delete_all(&mut self) -> Result<(), WalError> {
        (**self).delete_all()
    }
}

impl WalAppender for Box<dyn WalAppender> {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        (**self).append(batch)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("corrupt segment {index} at byte offset {offset}")]
    CorruptSegment { index: SegmentIndex, offset: u64 },

    #[error("WAL disk budget exceeded ({used} / {budget} bytes)")]
    DiskFull { budget: u64, used: u64 },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
