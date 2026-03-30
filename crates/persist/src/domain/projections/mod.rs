pub mod downsample;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;

use super::changeset::ChangeSet;

pub trait Projection: Send + Sync + 'static {
    fn apply(&mut self, run_id: RunId, batch: &MetricBatch, changeset: &mut ChangeSet);
}
