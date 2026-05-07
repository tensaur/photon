//! Per-subscription state and the shared routing index.
//!
//! Each subscription runs in its own task and owns its `SubscriptionState`
//! exclusively — no locks, no contention. The only shared mutable state is
//! `RouterIndex`, a lock-free DashMap-backed lookup that the service's
//! `handle_event` hot path uses to fan events out to the right subscriptions.

use std::sync::Arc;

use dashmap::DashMap;

use photon_core::types::bucket::Bucket;
use photon_core::types::id::{RunId, SubscriptionId};
use photon_core::types::metric::{Metric, MetricBatch, Step};
use photon_core::types::query::{DataPoint, MetricQuery, MetricSeries, SeriesData};
use photon_core::types::resolution::Resolution;
use photon_core::types::stream::SubscriptionUpdate;

use super::service::SubscriptionSender;

// ---------------------------------------------------------------------------
// RouterIndex — lock-free lookup from (run, metric) to subscribers.
// ---------------------------------------------------------------------------

/// Bidirectional subscriber lookup keyed by resolution. Shared between the
/// service's `handle_event` (reads) and per-subscription tasks (writes during
/// coarsen-induced reindex).
#[derive(Clone, Default)]
pub(super) struct RouterIndex {
    raw: Arc<DashMap<(RunId, Metric), Vec<SubscriptionId>>>,
    bucket: Arc<DashMap<(RunId, Metric, usize), Vec<SubscriptionId>>>,
}

impl RouterIndex {
    pub(super) fn add(
        &self,
        id: SubscriptionId,
        run_id: RunId,
        metric: &Metric,
        resolution: &Resolution,
    ) {
        match resolution {
            Resolution::Raw => {
                self.raw
                    .entry((run_id, metric.clone()))
                    .or_default()
                    .push(id);
            }
            Resolution::Bucketed(tier) => {
                self.bucket
                    .entry((run_id, metric.clone(), *tier))
                    .or_default()
                    .push(id);
            }
        }
    }

    pub(super) fn remove(
        &self,
        id: SubscriptionId,
        run_id: RunId,
        metric: &Metric,
        resolution: &Resolution,
    ) {
        match resolution {
            Resolution::Raw => {
                let key = (run_id, metric.clone());
                let empty = {
                    let Some(mut entry) = self.raw.get_mut(&key) else {
                        return;
                    };
                    entry.retain(|&x| x != id);
                    entry.is_empty()
                };
                if empty {
                    self.raw.remove(&key);
                }
            }
            Resolution::Bucketed(tier) => {
                let key = (run_id, metric.clone(), *tier);
                let empty = {
                    let Some(mut entry) = self.bucket.get_mut(&key) else {
                        return;
                    };
                    entry.retain(|&x| x != id);
                    entry.is_empty()
                };
                if empty {
                    self.bucket.remove(&key);
                }
            }
        }
    }

    pub(super) fn raw_subs(&self, run_id: RunId, metric: &Metric) -> Option<Vec<SubscriptionId>> {
        self.raw.get(&(run_id, metric.clone())).map(|v| v.clone())
    }

    pub(super) fn bucket_subs(
        &self,
        run_id: RunId,
        metric: &Metric,
        tier: usize,
    ) -> Option<Vec<SubscriptionId>> {
        self.bucket
            .get(&(run_id, metric.clone(), tier))
            .map(|v| v.clone())
    }
}

// ---------------------------------------------------------------------------
// SubscriptionState — one subscription's data, owned exclusively by its task.
// ---------------------------------------------------------------------------

pub(super) struct SubscriptionState {
    pub(super) id: SubscriptionId,
    pub(super) run_id: RunId,
    pub(super) metric: Metric,
    pub(super) target_points: usize,
    pub(super) current_resolution: Resolution,
    pub(super) emitted_through: Step,
    pub(super) sample_count: usize,
    /// `needs_coarsen` returns true when `sample_count` exceeds this.
    /// Reset to `1.5 * target_points` on every fresh snapshot; after a failed
    /// coarsen attempt, doubled to rate-limit retries exponentially.
    pub(super) coarsen_threshold: usize,
    pub(super) response_tx: SubscriptionSender,
}

impl SubscriptionState {
    pub(super) fn new(
        id: SubscriptionId,
        query: &MetricQuery,
        resolution: Resolution,
        response_tx: SubscriptionSender,
    ) -> Self {
        Self {
            id,
            run_id: query.run_id,
            metric: query.key.clone(),
            target_points: query.target_points,
            current_resolution: resolution,
            emitted_through: Step::ZERO,
            sample_count: 0,
            coarsen_threshold: (query.target_points as f64 * 1.5) as usize,
            response_tx,
        }
    }

    pub(super) fn needs_coarsen(&self) -> bool {
        self.target_points > 0 && self.sample_count > self.coarsen_threshold
    }

    /// Set trackers to match a freshly-read snapshot.
    pub(super) fn apply_snapshot(&mut self, data: &SeriesData) {
        match data {
            SeriesData::Raw { points } => {
                self.emitted_through = points.last().map_or(Step::ZERO, |p| p.step);
                self.sample_count = points.len();
            }
            SeriesData::Bucketed { buckets } => {
                self.emitted_through = buckets.last().map_or(Step::ZERO, |b| b.step_end);
                self.sample_count = buckets.len();
            }
        }
        self.coarsen_threshold = (self.target_points as f64 * 1.5) as usize;
    }

    /// Apply a `BatchDecoded` event. No-op if we're not Raw or it's for a
    /// different (run, metric). Returns true if the coarsen threshold was
    /// crossed.
    pub(super) fn apply_batch(&mut self, run_id: RunId, batch: &MetricBatch) -> bool {
        if !matches!(self.current_resolution, Resolution::Raw) {
            return false;
        }
        if run_id != self.run_id {
            return false;
        }
        let Some(key_index) = batch.keys.iter().position(|k| k == &self.metric) else {
            return false;
        };

        let new_points: Vec<DataPoint> = batch
            .points
            .iter()
            .filter(|p| p.key_index == key_index as u32 && p.step > self.emitted_through)
            .map(|p| DataPoint {
                step: p.step,
                value: p.value,
            })
            .collect();

        if new_points.is_empty() {
            return false;
        }

        self.emitted_through = new_points.last().unwrap().step;
        self.sample_count += new_points.len();
        let _ = self
            .response_tx
            .send((self.id, SubscriptionUpdate::DeltaPoints(new_points)));
        self.needs_coarsen()
    }

    /// Apply a `BucketsReduced` event. No-op if we're not at the matching
    /// tier or it's for a different (run, metric). Returns true if the
    /// coarsen threshold was crossed.
    pub(super) fn apply_bucket(
        &mut self,
        run_id: RunId,
        key: &Metric,
        tier: usize,
        bucket: Bucket,
    ) -> bool {
        let Resolution::Bucketed(my_tier) = self.current_resolution else {
            return false;
        };
        if my_tier != tier || run_id != self.run_id || key != &self.metric {
            return false;
        }
        if bucket.step_end <= self.emitted_through {
            return false;
        }
        self.emitted_through = bucket.step_end;
        self.sample_count += 1;
        let _ = self
            .response_tx
            .send((self.id, SubscriptionUpdate::DeltaBuckets(vec![bucket])));
        self.needs_coarsen()
    }

    /// Send a replacement `Snapshot` (used after a successful coarsen).
    pub(super) fn send_snapshot(&self, series: MetricSeries) {
        let _ = self
            .response_tx
            .send((self.id, SubscriptionUpdate::Snapshot { series }));
    }

    /// Send the final `Unsubscribed` notification (used on explicit unsubscribe).
    pub(super) fn send_unsubscribed(&self) {
        let _ = self
            .response_tx
            .send((self.id, SubscriptionUpdate::Unsubscribed));
    }

    /// Double the coarsen threshold after a failed attempt — exponential
    /// backoff so we don't re-query the store on every event when no coarser
    /// tier qualifies.
    pub(super) fn defer_coarsen(&mut self) {
        self.coarsen_threshold = self.sample_count.saturating_mul(2);
    }
}
