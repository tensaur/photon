use std::sync::Arc;

use photon_core::domain::run::RunStatus;
use photon_core::types::bucket::Bucket;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};

use crate::IngestHook;

/// Fans out pipeline events to multiple hooks.
pub struct CompositeHook {
    hooks: Vec<Arc<dyn IngestHook>>,
}

impl CompositeHook {
    pub fn new(hooks: Vec<Arc<dyn IngestHook>>) -> Self {
        Self { hooks }
    }
}

impl IngestHook for CompositeHook {
    fn on_batch_decoded(&self, run_id: RunId, batch: &MetricBatch) {
        for hook in &self.hooks {
            hook.on_batch_decoded(run_id, batch);
        }
    }

    fn on_buckets_closed(&self, run_id: RunId, key: &Metric, tier: u64, bucket: &Bucket) {
        for hook in &self.hooks {
            hook.on_buckets_closed(run_id, key, tier, bucket);
        }
    }

    fn on_run_status_change(&self, run_id: RunId, old: &RunStatus, new: &RunStatus) {
        for hook in &self.hooks {
            hook.on_run_status_change(run_id, old, new);
        }
    }
}
