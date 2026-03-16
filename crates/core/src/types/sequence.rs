use std::fmt;

use serde::{Deserialize, Serialize};

/// Monotonically increasing batch identifier, scoped per run.
/// Used for ordering, deduplication, and WAL truncation watermarks.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn prev_or_zero(self) -> Self {
        let raw = u64::from(self);

        if raw == 0 {
            Self::ZERO
        } else {
            Self::from(raw - 1)
        }
    }
}

impl From<u64> for SequenceNumber {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<SequenceNumber> for u64 {
    fn from(s: SequenceNumber) -> Self {
        s.0
    }
}

impl fmt::Debug for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seq({})", self.0)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identifies a WAL segment file on disk. Monotonically increasing per run.
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
