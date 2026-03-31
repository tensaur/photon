use dashmap::DashMap;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Sequence matches `expected_next`. Accept and advance.
    Process,
    /// Sequence is below `expected_next`. Already processed.
    Duplicate,
    /// Sequence is above `expected_next`. Stream has a gap — session is broken.
    Gap,
}

/// Tracks the next expected sequence number per run.
#[derive(Clone)]
pub struct DeduplicationCache {
    expected_next: std::sync::Arc<DashMap<RunId, SequenceNumber>>,
}

impl Default for DeduplicationCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DeduplicationCache {
    pub fn new() -> Self {
        Self {
            expected_next: std::sync::Arc::new(DashMap::new()),
        }
    }

    /// Seed `expected_next` from persisted watermarks and WAL.
    pub fn seed(&self, entries: &[(RunId, SequenceNumber)]) {
        for (run_id, seq) in entries {
            self.expected_next
                .entry(*run_id)
                .and_modify(|existing| {
                    let next = seq.next();
                    if next > *existing {
                        *existing = next;
                    }
                })
                .or_insert_with(|| seq.next());
        }
    }

    pub fn check(&self, run_id: &RunId, seq: SequenceNumber) -> Verdict {
        let expected = self
            .expected_next
            .get(run_id)
            .map_or(SequenceNumber::from(1), |w| *w.value());

        match seq.cmp(&expected) {
            std::cmp::Ordering::Equal => Verdict::Process,
            std::cmp::Ordering::Less => Verdict::Duplicate,
            std::cmp::Ordering::Greater => Verdict::Gap,
        }
    }

    pub fn watermark(&self, run_id: &RunId) -> SequenceNumber {
        let expected = self
            .expected_next
            .get(run_id)
            .map_or(SequenceNumber::from(1), |w| *w.value());
        expected.prev_or_zero()
    }

    pub fn advance(&self, run_id: &RunId, seq: SequenceNumber) {
        self.expected_next.insert(*run_id, seq.next());
    }

    pub fn evict(&self, run_id: &RunId) {
        self.expected_next.remove(run_id);
    }
}
