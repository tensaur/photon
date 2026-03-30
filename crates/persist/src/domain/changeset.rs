use std::collections::HashMap;
use std::sync::Arc;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;

/// Collected output of a single write cycle.
///
/// The decode phase produces `decoded_batches` and `watermarks`.
/// Derived projections (e.g. downsample) append to `bucket_entries`.
/// Events are queued during the cycle and published only after flush.
pub struct ChangeSet {
    pub decoded_batches: Vec<MetricBatch>,
    pub watermarks: HashMap<RunId, SequenceNumber>,
    pub bucket_entries: Vec<BucketEntry>,
    pub events: Vec<PhotonEvent>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self {
            decoded_batches: Vec::new(),
            watermarks: HashMap::new(),
            bucket_entries: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn with_capacity(batch_count: usize) -> Self {
        Self {
            decoded_batches: Vec::with_capacity(batch_count),
            watermarks: HashMap::new(),
            bucket_entries: Vec::new(),
            events: Vec::new(),
        }
    }

    /// Record a decoded batch and its watermark. Queues a `BatchDecoded` event.
    pub fn add_decoded_batch(
        &mut self,
        run_id: RunId,
        seq: SequenceNumber,
        batch: MetricBatch,
    ) {
        self.watermarks
            .entry(run_id)
            .and_modify(|s| {
                if seq > *s {
                    *s = seq;
                }
            })
            .or_insert(seq);

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
