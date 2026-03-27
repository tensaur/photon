use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    Process,
    Duplicate,
}

/// In-memory cache of the highest sequence number seen per run.
#[derive(Clone)]
pub struct DeduplicationCache {
    seen: std::sync::Arc<DashMap<RunId, SequenceNumber>>,
}

impl Default for DeduplicationCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DeduplicationCache {
    pub fn new() -> Self {
        Self {
            seen: std::sync::Arc::new(DashMap::new()),
        }
    }

    pub fn seed(&self, entries: &[(RunId, SequenceNumber)]) {
        for (run_id, seq) in entries {
            self.seen.insert(*run_id, *seq);
        }
    }

    pub fn check(&self, run_id: &RunId, seq: SequenceNumber) -> Verdict {
        let highest = self
            .seen
            .get(run_id)
            .map_or(SequenceNumber::ZERO, |w| *w.value());

        if seq <= highest {
            Verdict::Duplicate
        } else {
            Verdict::Process
        }
    }

    pub fn watermark(&self, run_id: &RunId) -> SequenceNumber {
        self.seen
            .get(run_id)
            .map_or(SequenceNumber::ZERO, |w| *w.value())
    }

    pub fn advance(&self, run_id: &RunId, seq: SequenceNumber) {
        self.seen.insert(*run_id, seq);
    }

    pub fn evict(&self, run_id: &RunId) {
        self.seen.remove(run_id);
    }
}
