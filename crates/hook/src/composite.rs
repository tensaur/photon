use std::sync::Arc;

use crate::ports::hook::{PipelineHook, RunManifest, TriggerEvent};
use crate::{MetricBatch, RunId, RunStatus};

/// Fans out pipeline events to multiple hooks.
pub struct CompositeHook {
    hooks: Vec<Arc<dyn PipelineHook>>,
}

impl CompositeHook {
    pub fn new(hooks: Vec<Arc<dyn PipelineHook>>) -> Self {
        Self { hooks }
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }

    pub fn len(&self) -> usize {
        self.hooks.len()
    }
}

impl PipelineHook for CompositeHook {
    fn on_metrics_written(&self, run_id: RunId, batch: Arc<MetricBatch>) {
        for hook in &self.hooks {
            hook.on_metrics_written(run_id, Arc::clone(&batch));
        }
    }

    fn on_trigger(&self, event: Arc<TriggerEvent>) {
        for hook in &self.hooks {
            hook.on_trigger(Arc::clone(&event));
        }
    }

    fn on_run_status_change(&self, run_id: RunId, old: RunStatus, new: RunStatus) {
        for hook in &self.hooks {
            hook.on_run_status_change(run_id, old.clone(), new.clone());
        }
    }

    fn on_manifest_update(&self, manifest: Arc<RunManifest>) {
        for hook in &self.hooks {
            hook.on_manifest_update(Arc::clone(&manifest));
        }
    }
}
