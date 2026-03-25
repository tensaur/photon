use std::fmt;

use serde::{Deserialize, Serialize};

/// Position in a WAL.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WalOffset(u64);

impl WalOffset {
    pub const ZERO: Self = Self(0);

    pub fn advance(self, count: u64) -> Self {
        Self(self.0 + count)
    }
}

impl From<u64> for WalOffset {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<WalOffset> for u64 {
    fn from(o: WalOffset) -> Self {
        o.0
    }
}

impl fmt::Debug for WalOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Off({})", self.0)
    }
}

impl fmt::Display for WalOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a WAL segment file on disk.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentIndex(u64);

impl SegmentIndex {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl From<u64> for SegmentIndex {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<SegmentIndex> for u64 {
    fn from(s: SegmentIndex) -> Self {
        s.0
    }
}

impl fmt::Debug for SegmentIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seg({})", self.0)
    }
}

impl fmt::Display for SegmentIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Zero-padded to match WAL file naming: segment-000001.wal
        write!(f, "{:06}", self.0)
    }
}
