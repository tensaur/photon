use photon_core::types::bucket::Bucket;

use crate::ports::aggregator::Aggregator;

/// M4 aggregation: preserves first, last, min, and max per bucket.
#[derive(Clone)]
pub struct M4Aggregator;

#[derive(Clone, Debug)]
pub struct M4Bucket {
    first_step: u64,
    first_value: f64,
    last_step: u64,
    last_value: f64,
    min_value: f64,
    max_value: f64,
}

impl Aggregator for M4Aggregator {
    type Bucket = M4Bucket;

    fn new_bucket(&self, step: u64, value: f64) -> M4Bucket {
        M4Bucket {
            first_step: step,
            first_value: value,
            last_step: step,
            last_value: value,
            min_value: value,
            max_value: value,
        }
    }

    fn push(&self, bucket: &mut M4Bucket, step: u64, value: f64) {
        bucket.last_step = step;
        bucket.last_value = value;
        bucket.min_value = bucket.min_value.min(value);
        bucket.max_value = bucket.max_value.max(value);
    }

    fn merge(&self, a: &M4Bucket, b: &M4Bucket) -> M4Bucket {
        M4Bucket {
            first_step: a.first_step,
            first_value: a.first_value,
            last_step: b.last_step,
            last_value: b.last_value,
            min_value: a.min_value.min(b.min_value),
            max_value: a.max_value.max(b.max_value),
        }
    }

    fn close(&self, bucket: &M4Bucket, step: u64) -> Bucket {
        Bucket {
            step,
            value: bucket.last_value,
            min: bucket.min_value,
            max: bucket.max_value,
        }
    }
}

