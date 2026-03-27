use photon_core::types::bucket::Bucket;
use photon_core::types::metric::Step;

/// Incremental bucketing for the resolution pyramid.
///
/// Aggregators use a private internal bucket type for accumulation and
/// merging. When a bucket closes, [`Aggregator::close`] extracts the
/// summary into a [`Bucket`].
///
/// [`Aggregator::merge`] must be associative.
pub trait Aggregator: Send + Sync + Clone + 'static {
    type Bucket: Clone + Send + Sync;

    /// Create a new bucket from the first observation.
    fn new_bucket(&self, step: Step, value: f64) -> Self::Bucket;

    /// Add a point to an open bucket.
    fn push(&self, bucket: &mut Self::Bucket, step: Step, value: f64);

    /// Combine two buckets. Must be associative.
    fn merge(&self, a: &Self::Bucket, b: &Self::Bucket) -> Self::Bucket;

    /// Extract the final summary. Called once when a bucket closes.
    fn close(&self, bucket: &Self::Bucket, step_start: Step, step_end: Step) -> Bucket;
}
