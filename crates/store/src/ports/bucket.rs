use std::future::Future;
use std::ops::Range;

use photon_core::types::bucket::{Bucket, BucketEntry};
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};

use super::{ReadError, WriteError};

/// Batched writes of pre-aggregated buckets.
pub trait BucketWriter: Send + Sync + Clone + 'static {
    fn write_buckets(
        &self,
        run_id: &RunId,
        entries: &[BucketEntry],
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}

/// Range reads over pre-aggregated buckets at a given tier.
pub trait BucketReader: Send + Sync + Clone + 'static {
    fn read_buckets(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        step_range: Range<Step>,
    ) -> impl Future<Output = Result<Vec<Bucket>, ReadError>> + Send;
}
