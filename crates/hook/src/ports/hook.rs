use std::sync::Arc;

use crate::{Metric, MetricBatch, RunId, RunStatus};

/// A trigger event fired by a pipeline hook.
#[derive(Clone, Debug)]
pub struct TriggerEvent {
    pub run_id: RunId,
    pub key: Metric,
    pub message: String,
}

/// Summary of a run's current metric state.
#[derive(Clone, Debug)]
pub struct RunManifest {
    pub run_id: RunId,
    pub keys: Vec<Metric>,
    pub total_points: u64,
    pub latest_step: u64,
}

/// Outbound port for reacting to ingest pipeline events.
pub trait PipelineHook: Send + Sync + 'static {
    fn on_metrics_written(&self, _run_id: RunId, _batch: Arc<MetricBatch>) {}
    fn on_trigger(&self, _event: Arc<TriggerEvent>) {}
    fn on_run_status_change(&self, _run_id: RunId, _old: RunStatus, _new: RunStatus) {}
    fn on_manifest_update(&self, _manifest: Arc<RunManifest>) {}
}

