//! Internal state for the subscription service. Guarded by the service's
//! `Mutex`; every method here runs synchronously under the lock — no `.await`.

use std::collections::HashMap;
use std::ops::Range;

use photon_core::types::bucket::Bucket;
use photon_core::types::id::{RunId, SubscriptionId};
use photon_core::types::metric::{Metric, MetricBatch, Step};
use photon_core::types::query::{DataPoint, MetricQuery, MetricSeries, SeriesData};
use photon_core::types::resolution::Resolution;
use photon_core::types::stream::SubscriptionUpdate;

use super::service::SubscriptionSender;

pub(super) struct SubscriptionState {
    run_id: RunId,
    metric: Metric,
    range_start: Step,
    target_points: usize,
    current_resolution: Resolution,
    emitted_through: Step,
    sample_count: usize,
    /// `needs_coarsen` returns true when `sample_count` exceeds this.
    /// Rebased to the standard `1.5 * target_points` overshoot on each fresh
    /// snapshot. Pushed past the current `sample_count` by `defer_coarsen`
    /// after a failed coarsen attempt, to avoid re-querying the store on
    /// every subsequent event when no coarser tier qualifies.
    coarsen_threshold: usize,
    pub(super) response_tx: SubscriptionSender,
}

impl SubscriptionState {
    fn new(query: &MetricQuery, resolution: Resolution, response_tx: SubscriptionSender) -> Self {
        Self {
            run_id: query.run_id,
            metric: query.key.clone(),
            range_start: query.step_range.start,
            target_points: query.target_points,
            current_resolution: resolution,
            emitted_through: Step::ZERO,
            sample_count: 0,
            coarsen_threshold: (query.target_points as f64 * 1.5) as usize,
            response_tx,
        }
    }

    fn needs_coarsen(&self) -> bool {
        self.target_points > 0 && self.sample_count > self.coarsen_threshold
    }

    /// Set `emitted_through` and `sample_count` to match a freshly-read snapshot.
    /// Also resets `coarsen_threshold` to the standard overshoot, since the
    /// new series starts fresh.
    fn apply_snapshot(&mut self, data: &SeriesData) {
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
        self.coarsen_threshold =
            self.sample_count + (self.target_points as f64 * 0.5) as usize;
    }

    /// Filter `points` against `emitted_through`.
    /// Send what's new on the channel, and advance trackers.
    fn apply_delta_points(&mut self, id: SubscriptionId, points: &[DataPoint]) -> bool {
        let new_points: Vec<DataPoint> = points
            .iter()
            .filter(|p| p.step > self.emitted_through)
            .cloned()
            .collect();
        if new_points.is_empty() {
            return false;
        }
        self.emitted_through = new_points.last().unwrap().step;
        self.sample_count += new_points.len();
        let _ = self
            .response_tx
            .send((id, SubscriptionUpdate::DeltaPoints(new_points)));
        self.needs_coarsen()
    }

    /// Send a newly-closed bucket on the channel if it's beyond what's already
    /// been emitted. Returns true if the budget was crossed.
    fn apply_delta_bucket(&mut self, id: SubscriptionId, bucket: Bucket) -> bool {
        if bucket.step_end <= self.emitted_through {
            return false;
        }
        self.emitted_through = bucket.step_end;
        self.sample_count += 1;
        let _ = self
            .response_tx
            .send((id, SubscriptionUpdate::DeltaBuckets(vec![bucket])));
        self.needs_coarsen()
    }
}

struct SubscriberIndex {
    raw: HashMap<(RunId, Metric), Vec<SubscriptionId>>,
    bucket: HashMap<(RunId, Metric, usize), Vec<SubscriptionId>>,
}

impl SubscriberIndex {
    fn new() -> Self {
        Self {
            raw: HashMap::new(),
            bucket: HashMap::new(),
        }
    }

    fn add(
        &mut self,
        id: SubscriptionId,
        run_id: RunId,
        metric: &Metric,
        resolution: &Resolution,
    ) {
        match resolution {
            Resolution::Raw => self
                .raw
                .entry((run_id, metric.clone()))
                .or_default()
                .push(id),
            Resolution::Bucketed(tier) => self
                .bucket
                .entry((run_id, metric.clone(), *tier))
                .or_default()
                .push(id),
        }
    }

    fn remove(
        &mut self,
        id: SubscriptionId,
        run_id: RunId,
        metric: &Metric,
        resolution: &Resolution,
    ) {
        match resolution {
            Resolution::Raw => Self::remove_id(&mut self.raw, (run_id, metric.clone()), id),
            Resolution::Bucketed(tier) => {
                Self::remove_id(&mut self.bucket, (run_id, metric.clone(), *tier), id)
            }
        }
    }

    fn remove_id<K: Eq + std::hash::Hash>(
        index: &mut HashMap<K, Vec<SubscriptionId>>,
        key: K,
        id: SubscriptionId,
    ) {
        if let Some(ids) = index.get_mut(&key) {
            ids.retain(|&existing| existing != id);
            if ids.is_empty() {
                index.remove(&key);
            }
        }
    }

    fn raw_subs(&self, run_id: RunId, metric: &Metric) -> Option<Vec<SubscriptionId>> {
        self.raw.get(&(run_id, metric.clone())).cloned()
    }

    fn bucket_subs(
        &self,
        run_id: RunId,
        metric: &Metric,
        tier: usize,
    ) -> Option<Vec<SubscriptionId>> {
        self.bucket.get(&(run_id, metric.clone(), tier)).cloned()
    }
}

pub(super) struct CoarsenArgs {
    pub(super) run_id: RunId,
    pub(super) metric: Metric,
    pub(super) range: Range<Step>,
    pub(super) target_points: usize,
    pub(super) current: Resolution,
}

pub(super) struct State {
    subs: HashMap<SubscriptionId, SubscriptionState>,
    index: SubscriberIndex,
}

impl State {
    pub(super) fn new() -> Self {
        Self {
            subs: HashMap::new(),
            index: SubscriberIndex::new(),
        }
    }

    pub(super) fn register(
        &mut self,
        id: SubscriptionId,
        query: &MetricQuery,
        resolution: Resolution,
        series: &MetricSeries,
        response_tx: SubscriptionSender,
    ) {
        let mut sub = SubscriptionState::new(query, resolution.clone(), response_tx);
        sub.apply_snapshot(&series.data);
        self.index.add(id, query.run_id, &query.key, &resolution);
        self.subs.insert(id, sub);
    }

    pub(super) fn remove(&mut self, id: SubscriptionId) -> Option<SubscriptionState> {
        let sub = self.subs.remove(&id)?;
        self.index
            .remove(id, sub.run_id, &sub.metric, &sub.current_resolution);
        Some(sub)
    }

    pub(super) fn coarsen_args(&self, id: SubscriptionId) -> Option<CoarsenArgs> {
        let sub = self.subs.get(&id)?;
        Some(CoarsenArgs {
            run_id: sub.run_id,
            metric: sub.metric.clone(),
            range: sub.range_start..sub.emitted_through,
            target_points: sub.target_points,
            current: sub.current_resolution.clone(),
        })
    }

    /// Record that a coarsen attempt for `id` found no coarser tier. Push
    /// the threshold past the current `sample_count` so we don't re-query
    /// the store on every subsequent event until enough new samples arrive.
    pub(super) fn defer_coarsen(&mut self, id: SubscriptionId) {
        if let Some(sub) = self.subs.get_mut(&id) {
            sub.coarsen_threshold = sub.sample_count + sub.target_points;
        }
    }

    /// Swap a subscription to a new (coarser) resolution.
    pub(super) fn replace_resolution(
        &mut self,
        id: SubscriptionId,
        new_resolution: Resolution,
        series: MetricSeries,
    ) {
        let Some(sub) = self.subs.get_mut(&id) else {
            return;
        };
        let old_resolution =
            std::mem::replace(&mut sub.current_resolution, new_resolution.clone());
        sub.apply_snapshot(&series.data);
        let run_id = sub.run_id;
        let metric = sub.metric.clone();
        let _ = sub
            .response_tx
            .send((id, SubscriptionUpdate::Snapshot { series }));

        self.index.remove(id, run_id, &metric, &old_resolution);
        self.index.add(id, run_id, &metric, &new_resolution);
    }

    /// Route a decoded batch to all Raw subscriptions.
    pub(super) fn route_batch(
        &mut self,
        run_id: RunId,
        batch: &MetricBatch,
    ) -> Vec<SubscriptionId> {
        let mut coarsen = Vec::new();
        for (key_index, metric) in batch.keys.iter().enumerate() {
            let Some(sub_ids) = self.index.raw_subs(run_id, metric) else {
                continue;
            };
            let points: Vec<DataPoint> = batch
                .points
                .iter()
                .filter(|p| p.key_index == key_index as u32)
                .map(|p| DataPoint {
                    step: p.step,
                    value: p.value,
                })
                .collect();
            if points.is_empty() {
                continue;
            }
            for id in sub_ids {
                if let Some(sub) = self.subs.get_mut(&id) {
                    if sub.apply_delta_points(id, &points) {
                        coarsen.push(id);
                    }
                }
            }
        }
        coarsen
    }

    /// Route a closed bucket to all Bucketed subscriptions at its tier.
    pub(super) fn route_bucket(
        &mut self,
        run_id: RunId,
        key: &Metric,
        tier: usize,
        bucket: Bucket,
    ) -> Vec<SubscriptionId> {
        let mut coarsen = Vec::new();
        let Some(sub_ids) = self.index.bucket_subs(run_id, key, tier) else {
            return coarsen;
        };
        for id in sub_ids {
            if let Some(sub) = self.subs.get_mut(&id) {
                if sub.apply_delta_bucket(id, bucket.clone()) {
                    coarsen.push(id);
                }
            }
        }
        coarsen
    }
}
