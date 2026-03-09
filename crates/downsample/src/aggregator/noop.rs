use photon_core::types::bucket::Bucket;

use crate::ports::aggregator::Aggregator;

#[derive(Clone)]
pub struct NoOpAggregator;

#[derive(Clone)]
pub struct NoOpBucket {
    value: f64,
}

impl Aggregator for NoOpAggregator {
    type Bucket = NoOpBucket;

    fn new_bucket(&self, _step: u64, value: f64) -> NoOpBucket {
        NoOpBucket { value }
    }

    fn push(&self, bucket: &mut NoOpBucket, _step: u64, value: f64) {
        bucket.value = value;
    }

    fn merge(&self, _a: &NoOpBucket, b: &NoOpBucket) -> NoOpBucket {
        NoOpBucket { value: b.value }
    }

    fn close(&self, bucket: &NoOpBucket, step_start: u64, step_end: u64) -> Bucket {
        Bucket {
            step_start,
            step_end,
            value: bucket.value,
            min: bucket.value,
            max: bucket.value,
        }
    }
}
