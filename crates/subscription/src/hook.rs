use std::collections::HashSet;
use std::sync::Mutex;

use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::query::DataPoint;
use photon_core::types::subscription::SubscriptionEvent;
use photon_hook::IngestHook;
use photon_hook::broadcast::Broadcaster;

#[derive(Clone)]
pub struct SubscriptionHook {
    broadcaster: Broadcaster<SubscriptionEvent>,
    seen_runs: std::sync::Arc<Mutex<HashSet<RunId>>>,
}

impl SubscriptionHook {
    pub fn new() -> Self {
        Self {
            broadcaster: Broadcaster::new(256),
            seen_runs: std::sync::Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SubscriptionEvent> {
        self.broadcaster.subscribe()
    }
}

impl IngestHook for SubscriptionHook {
    fn on_batch_decoded(&self, run_id: RunId, batch: &MetricBatch) {
        let is_new = self
            .seen_runs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(run_id);

        if is_new {
            self.broadcaster.send(SubscriptionEvent::RunsChanged);
        }

        for (key_index, metric) in batch.keys.iter().enumerate() {
            let points: Vec<DataPoint> = batch
                .points
                .iter()
                .filter(|p| p.key_index == key_index as u32)
                .map(|p| DataPoint {
                    step: p.step,
                    value: p.value,
                })
                .collect();

            if !points.is_empty() {
                self.broadcaster.send(SubscriptionEvent::LivePoints {
                    run_id,
                    metric: metric.clone(),
                    points,
                });
            }
        }
    }
}
