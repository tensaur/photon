use std::collections::HashMap;
use std::sync::Arc;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;

/// Collected output for a single persist write cycle.
pub struct ChangeSet {
    pub decoded_batches: Vec<MetricBatch>,
    pub bucket_entries: Vec<BucketEntry>,
    pub watermarks: HashMap<RunId, SequenceNumber>,
    pub finalized_runs: Vec<RunId>,
    pub events: Vec<PhotonEvent>,
}

impl Default for ChangeSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeSet {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(batch_count: usize) -> Self {
        Self {
            decoded_batches: Vec::with_capacity(batch_count),
            bucket_entries: Vec::new(),
            watermarks: HashMap::new(),
            finalized_runs: Vec::new(),
            events: Vec::new(),
        }
    }

    /// Record a decoded batch. Queues a `BatchDecoded` event.
    pub fn add_decoded_batch(&mut self, run_id: RunId, batch: MetricBatch) {
        self.events.push(PhotonEvent::BatchDecoded {
            run_id,
            batch: Arc::new(batch.clone()),
        });
        self.decoded_batches.push(batch);
    }

    /// Record bucket entries produced by a projection. Queues `BucketsReduced`
    /// events for each entry.
    pub fn add_bucket_entries(&mut self, entries: Vec<BucketEntry>) {
        for entry in &entries {
            self.events.push(PhotonEvent::BucketsReduced {
                run_id: entry.run_id,
                key: entry.key.clone(),
                tier: entry.tier,
                bucket: entry.bucket.clone(),
            });
        }
        self.bucket_entries.extend(entries);
    }

    /// Mark a run as finalized. Queues a `Finalized` event.
    pub fn mark_finalized(&mut self, run_id: RunId) {
        self.finalized_runs.push(run_id);
        self.events.push(PhotonEvent::Finalized { run_id });
    }

    /// Track the latest persisted sequence number for a run in this cycle.
    pub fn add_watermark(&mut self, run_id: RunId, seq: SequenceNumber) {
        self.watermarks
            .entry(run_id)
            .and_modify(|current| {
                if seq > *current {
                    *current = seq;
                }
            })
            .or_insert(seq);
    }
}
