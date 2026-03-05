mod disk;
mod memory;
mod segment;

pub(crate) use self::disk::{DiskWalConfig, SharedDiskWal};
pub(crate) use self::memory::InMemoryWal;
