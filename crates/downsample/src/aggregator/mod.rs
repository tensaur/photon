pub mod m4;
pub mod noop;

use photon_core::types::bucket::Bucket;
use photon_core::types::metric::Step;

use crate::ports::aggregator::Aggregator;
use m4::{M4Aggregator, M4Bucket};
use noop::{NoOpAggregator, NoOpBucket};

/// Enum dispatch for aggregators, following the same pattern as
/// `CompressorKind` and `CodecKind`.
#[derive(Clone, Default)]
pub enum AggregatorKind {
    #[default]
    M4,
    NoOp,
}

#[derive(Clone)]
pub enum AggregatorBucket {
    M4(M4Bucket),
    NoOp(NoOpBucket),
}

impl Aggregator for AggregatorKind {
    type Bucket = AggregatorBucket;

    fn new_bucket(&self, step: Step, value: f64) -> AggregatorBucket {
        match self {
            Self::M4 => AggregatorBucket::M4(M4Aggregator.new_bucket(step, value)),
            Self::NoOp => AggregatorBucket::NoOp(NoOpAggregator.new_bucket(step, value)),
        }
    }

    fn push(&self, bucket: &mut AggregatorBucket, step: Step, value: f64) {
        match (self, bucket) {
            (Self::M4, AggregatorBucket::M4(b)) => M4Aggregator.push(b, step, value),
            (Self::NoOp, AggregatorBucket::NoOp(b)) => NoOpAggregator.push(b, step, value),
            _ => unreachable!("mismatched aggregator and bucket"),
        }
    }

    fn merge(&self, a: &AggregatorBucket, b: &AggregatorBucket) -> AggregatorBucket {
        match (self, a, b) {
            (Self::M4, AggregatorBucket::M4(a), AggregatorBucket::M4(b)) => {
                AggregatorBucket::M4(M4Aggregator.merge(a, b))
            }
            (Self::NoOp, AggregatorBucket::NoOp(a), AggregatorBucket::NoOp(b)) => {
                AggregatorBucket::NoOp(NoOpAggregator.merge(a, b))
            }
            _ => unreachable!("mismatched aggregator and bucket"),
        }
    }

    fn close(&self, bucket: &AggregatorBucket, step_start: Step, step_end: Step) -> Bucket {
        match (self, bucket) {
            (Self::M4, AggregatorBucket::M4(b)) => M4Aggregator.close(b, step_start, step_end),
            (Self::NoOp, AggregatorBucket::NoOp(b)) => {
                NoOpAggregator.close(b, step_start, step_end)
            }
            _ => unreachable!("mismatched aggregator and bucket"),
        }
    }
}
