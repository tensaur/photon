use std::collections::HashSet;
use std::sync::Mutex;

use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::query::DataPoint;
use photon_core::types::subscription::SubscriptionEvent;
use photon_hook::Hook;
use photon_hook::broadcast::Broadcaster;

/// Maps [`PhotonEvent`]s to [`SubscriptionEvent`]s for WebSocket clients.
///
/// Subscribes to the pipeline event broadcast and re-broadcasts as
/// subscription events. The WebSocket handler consumes the subscription
/// events to push live updates to clients.
#[derive(Clone)]
pub struct SubscriptionHook {
    broadcaster: Broadcaster<SubscriptionEvent>,
    seen_runs: std::sync::Arc<Mutex<HashSet<RunId>>>,
}

impl Default for SubscriptionHook {
    fn default() -> Self {
        Self::new()
    }
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

impl Hook for SubscriptionHook {
    fn handle(&self, event: PhotonEvent) {
        if let PhotonEvent::BatchDecoded { run_id, batch } = event {
            let is_new = self
                .seen_runs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
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
}
