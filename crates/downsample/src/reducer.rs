use std::collections::HashMap;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::metric::{Metric, MetricBatch};

use crate::ports::aggregator::Aggregator;
use crate::pyramid::Pyramid;

/// Routes metric batches across per-key pyramids.
/// Holds one [`Pyramid`] per metric key.
pub struct BatchReducer<A: Aggregator> {
    aggregator: A,
    tier_widths: Vec<u64>,
    keys: HashMap<Metric, Pyramid<A>>,
}

impl<A: Aggregator> BatchReducer<A> {
    pub fn new(aggregator: A, tier_widths: Vec<u64>) -> Self {
        Self {
            aggregator,
            tier_widths,
            keys: HashMap::new(),
        }
    }

    /// Process a batch. Returns all closed buckets across all keys and tiers.
    pub fn ingest(&mut self, batch: &MetricBatch) -> Vec<BucketEntry> {
        let mut entries = Vec::new();

        for point in &batch.points {
            let pyramid = self
                .keys
                .entry(point.key.clone())
                .or_insert_with(|| Pyramid::new(self.aggregator.clone(), self.tier_widths.clone()));

            for (tier, bucket) in pyramid.push(point.step, point.value) {
                entries.push(BucketEntry {
                    key: point.key.clone(),
                    tier,
                    bucket,
                });
            }
        }

        entries
    }
}
