mod disk;
mod memory;
mod segment;

pub use self::disk::{DiskWalConfig, SharedDiskWal};
pub use self::memory::InMemoryWal;
