use std::sync::Arc;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;

/// Collected projection output for a single write cycle.
pub struct ChangeSet {
    pub decoded_batches: Vec<MetricBatch>,
    pub bucket_entries: Vec<BucketEntry>,
    pub events: Vec<PhotonEvent>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self {
            decoded_batches: Vec::new(),
            bucket_entries: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn with_capacity(batch_count: usize) -> Self {
        Self {
            decoded_batches: Vec::with_capacity(batch_count),
            bucket_entries: Vec::new(),
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
}
