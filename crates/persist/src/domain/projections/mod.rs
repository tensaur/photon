pub mod downsample;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;

use super::changeset::ChangeSet;

/// Internal domain abstraction for streaming data transformations.
///
/// A projection processes decoded metric batches and accumulates output
/// into a [`ChangeSet`]. Projections are stateful — they may hold data
/// across batches (e.g. a reducer with open buckets) and only contribute
/// output when internal conditions are met.
///
/// Projections are pure domain logic: no I/O, no event awareness. The
/// service orchestrates flushing their output through outbound ports.
pub trait Projection: Send + Sync + 'static {
    /// Feed a decoded batch into this projection. Accumulated output
    /// should be written into the changeset.
    fn apply(&mut self, run_id: RunId, batch: &MetricBatch, changeset: &mut ChangeSet);
}
