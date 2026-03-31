pub mod downsample;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;

use super::changeset::ChangeSet;

/// Stateful streaming transform over decoded metric batches.
pub trait Projection: Send + Sync + 'static {
    fn project(&mut self, run_id: RunId, batch: &MetricBatch, changeset: &mut ChangeSet);
    fn finish_run(&mut self, run_id: RunId, changeset: &mut ChangeSet);
}
