pub mod composite;
pub mod noop;

pub use photon_core::domain::run::RunStatus;
pub use photon_core::types::bucket::Bucket;
pub use photon_core::types::id::RunId;
pub use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};

/// Extension point for reacting to ingest pipeline events.
///
/// # Example
///
/// ```ignore
/// use photon_hook::IngestHook;
///
/// struct MyHook;
///
/// impl IngestHook for MyHook {
///     fn on_batch_decoded(&self, run_id: RunId, batch: &MetricBatch) {
///         println!("run {run_id}: {} points", batch.len());
///     }
/// }
/// ```
pub trait IngestHook: Send + Sync + 'static {
    /// Called after a batch has been decompressed and decoded into domain types.
    fn on_batch_decoded(&self, _run_id: RunId, _batch: &MetricBatch) {}

    /// Called for each bucket that closed during aggregation.
    fn on_buckets_closed(&self, _run_id: RunId, _key: &Metric, _tier: u64, _bucket: &Bucket) {}

    /// Called when a run transitions between states.
    fn on_run_status_change(&self, _run_id: RunId, _old: &RunStatus, _new: &RunStatus) {}
}
