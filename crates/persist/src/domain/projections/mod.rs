pub mod downsample;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;

/// Internal domain abstraction for streaming data transformations.
///
/// A projection processes decoded metric batches and produces typed output
/// to be persisted. Projections are stateful — they may accumulate data
/// across batches (e.g. a reducer holding open buckets) and only emit
/// output when internal conditions are met.
///
/// Projections are pure domain logic: no I/O, no event awareness. The
/// service orchestrates writing their output through outbound ports.
pub trait Projection: Send + Sync + 'static {
    type Output;

    /// Feed a decoded batch into this projection. May accumulate state.
    fn process(&mut self, run_id: RunId, batch: &MetricBatch);

    /// Drain any accumulated output ready to be persisted.
    fn drain(&mut self) -> Self::Output;
}
