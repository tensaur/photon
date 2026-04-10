use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{broadcast, mpsc};

use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};
use photon_core::types::id::SubscriptionId;
use photon_core::types::query::{DataPoint, MetricQuery, MetricSeries, SeriesData};
use photon_core::types::stream::{DeltaData, SubscriptionUpdate};

/// Per-connection channel a `SubscriptionManager` uses to push updates back
/// to the WS handler. The handler is responsible for wrapping each pair into
/// a `StreamFrame::Subscription { id, update }` for transmission.
pub type SubscriptionSender = mpsc::UnboundedSender<(SubscriptionId, SubscriptionUpdate)>;
use photon_store::ports::bucket::BucketReader;
use photon_store::ports::metric::MetricReader;

use super::tier::{Lod, TierSelector};

struct SubscriptionState {
    run_id: RunId,
    metric: Metric,
    range_start: Step,
    target_points: usize,
    current_lod: Lod,
    emitted_through: Step,
    sample_count: usize,
    response_tx: SubscriptionSender,
}

pub enum ManagerCommand {
    Subscribe {
        query: MetricQuery,
        response_tx: SubscriptionSender,
    },
    Unsubscribe(SubscriptionId),
    /// A WebSocket connection has closed; tear down all of its subscriptions in
    /// one go. Skips the per-subscription `Unsubscribed` notification since the
    /// receiving channel is already gone.
    Disconnect(Vec<SubscriptionId>),
}

pub struct SubscriptionManager<B, M> {
    subscriptions: HashMap<SubscriptionId, SubscriptionState>,
    next_id: AtomicU64,

    // Indexes for O(1) event routing
    raw_index: HashMap<(RunId, Metric), Vec<SubscriptionId>>,
    bucket_index: HashMap<(RunId, Metric, usize), Vec<SubscriptionId>>,

    bucket_reader: B,
    metric_reader: M,
    tier_selector: TierSelector,
}

impl<B: BucketReader, M: MetricReader> SubscriptionManager<B, M> {
    pub fn new(bucket_reader: B, metric_reader: M, tier_selector: TierSelector) -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: AtomicU64::new(1),
            raw_index: HashMap::new(),
            bucket_index: HashMap::new(),
            bucket_reader,
            metric_reader,
            tier_selector,
        }
    }

    pub async fn run(
        mut self,
        mut event_rx: broadcast::Receiver<PhotonEvent>,
        mut cmd_rx: mpsc::UnboundedReceiver<ManagerCommand>,
    ) {
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ManagerCommand::Subscribe { query, response_tx }) => {
                            self.handle_subscribe(query, response_tx).await;
                        }
                        Some(ManagerCommand::Unsubscribe(id)) => {
                            self.handle_unsubscribe(id);
                        }
                        Some(ManagerCommand::Disconnect(ids)) => {
                            for id in ids {
                                self.remove_subscription(id);
                            }
                        }
                        None => break,
                    }
                }
                event = event_rx.recv() => {
                    match event {
                        Ok(ev) => {
                            let coarsen_candidates = self.handle_event(ev);
                            for sub_id in coarsen_candidates {
                                self.try_coarsen(sub_id).await;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("subscription manager lagged by {n} events");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    }

    async fn handle_subscribe(&mut self, query: MetricQuery, response_tx: SubscriptionSender) {
        let id = SubscriptionId::new(self.next_id.fetch_add(1, Ordering::Relaxed));

        let point_count = match self
            .metric_reader
            .count_points(&query.run_id, &query.key, query.step_range.clone())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("subscribe count_points failed: {e}");
                return;
            }
        };

        let lod = self.tier_selector.pick(point_count, query.target_points);

        let snapshot = match &lod {
            Lod::Raw => {
                match self
                    .metric_reader
                    .read_points(&query.run_id, &query.key, query.step_range.clone())
                    .await
                {
                    Ok(points) => SeriesData::Raw {
                        points: points
                            .into_iter()
                            .map(|(step, value)| DataPoint { step, value })
                            .collect(),
                    },
                    Err(e) => {
                        tracing::error!("subscribe read_points failed: {e}");
                        return;
                    }
                }
            }
            Lod::Bucketed(tier) => {
                match self
                    .bucket_reader
                    .read_buckets(&query.run_id, &query.key, *tier, query.step_range.clone())
                    .await
                {
                    Ok(buckets) => SeriesData::Bucketed { buckets },
                    Err(e) => {
                        tracing::error!("subscribe read_buckets failed: {e}");
                        return;
                    }
                }
            }
        };

        let emitted_through = snapshot_end(&snapshot);
        let sample_count = snapshot_len(&snapshot);

        let series = MetricSeries {
            run_id: query.run_id,
            key: query.key.clone(),
            data: snapshot,
        };

        let _ = response_tx.send((id, SubscriptionUpdate::Snapshot { series }));

        let state = SubscriptionState {
            run_id: query.run_id,
            metric: query.key.clone(),
            range_start: query.step_range.start,
            target_points: query.target_points,
            current_lod: lod.clone(),
            emitted_through,
            sample_count,
            response_tx,
        };

        // Insert into the appropriate index
        match &lod {
            Lod::Raw => {
                self.raw_index
                    .entry((query.run_id, query.key))
                    .or_default()
                    .push(id);
            }
            Lod::Bucketed(tier) => {
                self.bucket_index
                    .entry((query.run_id, query.key, *tier))
                    .or_default()
                    .push(id);
            }
        }

        self.subscriptions.insert(id, state);
    }

    fn handle_unsubscribe(&mut self, id: SubscriptionId) {
        if let Some(state) = self.remove_subscription(id) {
            let _ = state.response_tx.send((id, SubscriptionUpdate::Unsubscribed));
        }
    }

    /// Remove a subscription from the lookup map and from whichever LOD index
    /// it currently lives in. Returns the removed `SubscriptionState` so the
    /// caller can decide whether to send a notification (yes for explicit
    /// `Unsubscribe`, no for `Disconnect` since the channel is dead).
    fn remove_subscription(&mut self, id: SubscriptionId) -> Option<SubscriptionState> {
        let state = self.subscriptions.remove(&id)?;
        match &state.current_lod {
            Lod::Raw => {
                remove_from_index(
                    &mut self.raw_index,
                    (state.run_id, state.metric.clone()),
                    id,
                );
            }
            Lod::Bucketed(tier) => {
                remove_from_index(
                    &mut self.bucket_index,
                    (state.run_id, state.metric.clone(), *tier),
                    id,
                );
            }
        }
        Some(state)
    }

    /// Handle a pipeline event, returning subscription IDs that may need coarsening.
    fn handle_event(&mut self, event: PhotonEvent) -> Vec<SubscriptionId> {
        let mut coarsen_candidates = Vec::new();

        match event {
            PhotonEvent::BatchDecoded { run_id, batch } => {
                for (key_index, metric) in batch.keys.iter().enumerate() {
                    let key = (run_id, metric.clone());
                    let Some(sub_ids) = self.raw_index.get(&key) else {
                        continue;
                    };
                    let sub_ids: Vec<SubscriptionId> = sub_ids.clone();

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

                    for sub_id in sub_ids {
                        let Some(state) = self.subscriptions.get_mut(&sub_id) else {
                            continue;
                        };

                        let new_points: Vec<DataPoint> = points
                            .iter()
                            .filter(|p| p.step > state.emitted_through)
                            .cloned()
                            .collect();

                        if new_points.is_empty() {
                            continue;
                        }

                        if let Some(last) = new_points.last() {
                            state.emitted_through = last.step;
                        }
                        state.sample_count += new_points.len();

                        let _ = state.response_tx.send((
                            sub_id,
                            SubscriptionUpdate::Delta(DeltaData::RawPoints(new_points)),
                        ));

                        if needs_coarsen(state) {
                            coarsen_candidates.push(sub_id);
                        }
                    }
                }
            }
            PhotonEvent::BucketsReduced {
                run_id,
                key,
                tier,
                bucket,
            } => {
                let index_key = (run_id, key, tier);
                let Some(sub_ids) = self.bucket_index.get(&index_key) else {
                    return coarsen_candidates;
                };
                let sub_ids: Vec<SubscriptionId> = sub_ids.clone();

                for sub_id in sub_ids {
                    let Some(state) = self.subscriptions.get_mut(&sub_id) else {
                        continue;
                    };

                    if bucket.step_end <= state.emitted_through {
                        continue;
                    }

                    state.emitted_through = bucket.step_end;
                    state.sample_count += 1;

                    let _ = state.response_tx.send((
                        sub_id,
                        SubscriptionUpdate::Delta(DeltaData::Buckets(vec![bucket.clone()])),
                    ));

                    if needs_coarsen(state) {
                        coarsen_candidates.push(sub_id);
                    }
                }
            }
            // `PhotonEvent::Finalized` is handled directly by the WS handler,
            // not by the manager — it's a connection-broadcast event, not a
            // per-subscription update. Same goes for `RunStatusChanged`.
            _ => {}
        }

        coarsen_candidates
    }

    async fn try_coarsen(&mut self, sub_id: SubscriptionId) {
        let Some(state) = self.subscriptions.get(&sub_id) else {
            return;
        };

        let total = match self
            .metric_reader
            .count_points(
                &state.run_id,
                &state.metric,
                state.range_start..state.emitted_through,
            )
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("coarsen count_points failed: {e}");
                return;
            }
        };

        let new_lod = self.tier_selector.pick(total, state.target_points);

        if !new_lod.is_coarser_than(&state.current_lod) {
            return;
        }

        // Read fresh snapshot at coarser tier
        let Lod::Bucketed(tier) = &new_lod else {
            return; // Can't coarsen to Raw
        };

        let buckets = match self
            .bucket_reader
            .read_buckets(
                &state.run_id,
                &state.metric,
                *tier,
                state.range_start..state.emitted_through,
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("coarsen read_buckets failed: {e}");
                return;
            }
        };

        let snapshot_count = buckets.len();
        let series = MetricSeries {
            run_id: state.run_id,
            key: state.metric.clone(),
            data: SeriesData::Bucketed { buckets },
        };

        let _ = state
            .response_tx
            .send((sub_id, SubscriptionUpdate::Snapshot { series }));

        // Move subscription between indexes
        let old_lod = state.current_lod.clone();
        let run_id = state.run_id;
        let metric = state.metric.clone();

        // Update state (re-borrow mutably)
        let state = self.subscriptions.get_mut(&sub_id).unwrap();
        state.current_lod = new_lod.clone();
        state.sample_count = snapshot_count;

        // Remove from old index, add to new
        match &old_lod {
            Lod::Raw => {
                remove_from_index(&mut self.raw_index, (run_id, metric.clone()), sub_id);
            }
            Lod::Bucketed(old_tier) => {
                remove_from_index(
                    &mut self.bucket_index,
                    (run_id, metric.clone(), *old_tier),
                    sub_id,
                );
            }
        }
        match &new_lod {
            Lod::Raw => {
                self.raw_index
                    .entry((run_id, metric))
                    .or_default()
                    .push(sub_id);
            }
            Lod::Bucketed(new_tier) => {
                self.bucket_index
                    .entry((run_id, metric, *new_tier))
                    .or_default()
                    .push(sub_id);
            }
        }
    }
}

fn needs_coarsen(state: &SubscriptionState) -> bool {
    state.target_points > 0 && state.sample_count > (state.target_points as f64 * 1.5) as usize
}

fn snapshot_end(data: &SeriesData) -> Step {
    match data {
        SeriesData::Raw { points } => points.last().map_or(Step::ZERO, |p| p.step),
        SeriesData::Bucketed { buckets } => buckets.last().map_or(Step::ZERO, |b| b.step_end),
    }
}

fn snapshot_len(data: &SeriesData) -> usize {
    match data {
        SeriesData::Raw { points } => points.len(),
        SeriesData::Bucketed { buckets } => buckets.len(),
    }
}

fn remove_from_index<K: Eq + std::hash::Hash>(
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use photon_core::types::bucket::{Bucket, BucketEntry};
    use photon_core::types::metric::{MetricBatch, MetricPoint};

    use photon_store::memory::bucket::InMemoryBucketStore;
    use photon_store::memory::metric::InMemoryMetricStore;
    use photon_store::ports::bucket::BucketWriter;
    use photon_store::ports::metric::MetricWriter;

    use super::*;

    fn tier_selector() -> TierSelector {
        // Width 5 so we can test bucketed easily
        TierSelector::new(vec![5])
    }

    #[tokio::test]
    async fn subscribe_raw_snapshot() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();
        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..3)
                .map(|i| MetricPoint {
                    key_index: 0,
                    step: Step::new(i),
                    value: i as f64,
                    timestamp_ms: 0,
                })
                .collect(),
        };
        metrics.write_batch(&batch).await.unwrap();

        let (event_tx, _) = broadcast::channel(16);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();

        let manager = SubscriptionManager::new(buckets, metrics, tier_selector());
        let handle = tokio::spawn(manager.run(event_tx.subscribe(), cmd_rx));

        cmd_tx
            .send(ManagerCommand::Subscribe {
                query: MetricQuery {
                    run_id,
                    key: metric,
                    step_range: Step::ZERO..Step::MAX,
                    target_points: 500,
                },
                response_tx: resp_tx,
            })
            .unwrap();

        let msg = resp_rx.recv().await.unwrap();
        match msg {
            (_, SubscriptionUpdate::Snapshot { series }) => {
                assert!(matches!(series.data, SeriesData::Raw { ref points } if points.len() == 3));
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn raw_delta_after_subscribe() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        let (event_tx, _) = broadcast::channel(16);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();

        let manager = SubscriptionManager::new(buckets, metrics, tier_selector());
        let handle = tokio::spawn(manager.run(event_tx.subscribe(), cmd_rx));

        cmd_tx
            .send(ManagerCommand::Subscribe {
                query: MetricQuery {
                    run_id,
                    key: metric.clone(),
                    step_range: Step::ZERO..Step::MAX,
                    target_points: 500,
                },
                response_tx: resp_tx,
            })
            .unwrap();

        // Consume snapshot
        let _ = resp_rx.recv().await.unwrap();

        // Send a BatchDecoded event
        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: vec![MetricPoint {
                key_index: 0,
                step: Step::new(10),
                value: 0.5,
                timestamp_ms: 0,
            }],
        };
        event_tx
            .send(PhotonEvent::BatchDecoded {
                run_id,
                batch: Arc::new(batch),
            })
            .unwrap();

        let msg = resp_rx.recv().await.unwrap();
        match msg {
            (_, SubscriptionUpdate::Delta(DeltaData::RawPoints(points))) => {
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].step, Step::new(10));
            }
            other => panic!("expected raw Delta, got {other:?}"),
        }

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn bucketed_delta_after_subscribe() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        // Write enough raw points that the planner picks Bucketed(0) with width 5
        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..5000)
                .map(|i| MetricPoint {
                    key_index: 0,
                    step: Step::new(i),
                    value: i as f64,
                    timestamp_ms: 0,
                })
                .collect(),
        };
        metrics.write_batch(&batch).await.unwrap();

        // Write some closed buckets
        let bucket_entries: Vec<BucketEntry> = (0..1000)
            .map(|i| BucketEntry {
                run_id,
                key: metric.clone(),
                tier: 0,
                bucket: Bucket {
                    step_start: Step::new(i * 5),
                    step_end: Step::new(i * 5 + 4),
                    value: (i * 5) as f64,
                    count: 5,
                    min: (i * 5) as f64,
                    max: (i * 5 + 4) as f64,
                },
            })
            .collect();
        buckets.write_buckets(&bucket_entries).await.unwrap();

        let (event_tx, _) = broadcast::channel(16);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();

        let manager = SubscriptionManager::new(buckets, metrics, tier_selector());
        let handle = tokio::spawn(manager.run(event_tx.subscribe(), cmd_rx));

        cmd_tx
            .send(ManagerCommand::Subscribe {
                query: MetricQuery {
                    run_id,
                    key: metric.clone(),
                    step_range: Step::ZERO..Step::MAX,
                    target_points: 500,
                },
                response_tx: resp_tx,
            })
            .unwrap();

        // Consume bucketed snapshot
        let msg = resp_rx.recv().await.unwrap();
        assert!(matches!(msg, (_, SubscriptionUpdate::Snapshot { ref series })
            if matches!(series.data, SeriesData::Bucketed { ref buckets } if buckets.len() == 1000)));

        // Send a BucketsReduced event
        let new_bucket = Bucket {
            step_start: Step::new(5000),
            step_end: Step::new(5004),
            value: 5002.0,
            count: 5,
            min: 5000.0,
            max: 5004.0,
        };
        event_tx
            .send(PhotonEvent::BucketsReduced {
                run_id,
                key: metric,
                tier: 0,
                bucket: new_bucket.clone(),
            })
            .unwrap();

        let msg = resp_rx.recv().await.unwrap();
        match msg {
            (_, SubscriptionUpdate::Delta(DeltaData::Buckets(b))) => {
                assert_eq!(b.len(), 1);
                assert_eq!(b[0], new_bucket);
            }
            other => panic!("expected bucket Delta, got {other:?}"),
        }

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn disconnect_drops_all_subs_and_stops_routing_events() {
        // Simulate a WS connection closing: open multiple subscriptions on the
        // same response channel, send Disconnect with their ids, then publish
        // an event for one of those metrics. The manager must not try to route
        // to the dropped subscriptions (which would be a leak under the
        // small-runs workload).
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let m1 = Metric::new("loss").unwrap();
        let m2 = Metric::new("acc").unwrap();

        let (event_tx, _) = broadcast::channel(16);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();

        let manager = SubscriptionManager::new(buckets, metrics, tier_selector());
        let handle = tokio::spawn(manager.run(event_tx.subscribe(), cmd_rx));

        for metric in [&m1, &m2] {
            cmd_tx
                .send(ManagerCommand::Subscribe {
                    query: MetricQuery {
                        run_id,
                        key: metric.clone(),
                        step_range: Step::ZERO..Step::MAX,
                        target_points: 500,
                        },
                    response_tx: resp_tx.clone(),
                })
                .unwrap();
        }

        // Capture the assigned ids from the two snapshots.
        let mut ids = Vec::new();
        for _ in 0..2 {
            match resp_rx.recv().await.unwrap() {
                (id, SubscriptionUpdate::Snapshot { .. }) => ids.push(id),
                other => panic!("expected Snapshot, got {other:?}"),
            }
        }

        // Tear down via Disconnect — no per-sub Unsubscribed notification is
        // expected (the WS connection is gone in the real flow).
        cmd_tx.send(ManagerCommand::Disconnect(ids)).unwrap();

        // Give the manager a tick to process the Disconnect before we publish
        // an event. We can't introspect the manager's internal state directly,
        // so we round-trip a Subscribe→Snapshot to confirm the manager has
        // drained the Disconnect from its command queue.
        let m3 = Metric::new("acc2").unwrap();
        cmd_tx
            .send(ManagerCommand::Subscribe {
                query: MetricQuery {
                    run_id,
                    key: m3.clone(),
                    step_range: Step::ZERO..Step::MAX,
                    target_points: 500,
                },
                response_tx: resp_tx,
            })
            .unwrap();
        let drain_id = match resp_rx.recv().await.unwrap() {
            (id, SubscriptionUpdate::Snapshot { .. }) => id,
            other => panic!("expected Snapshot, got {other:?}"),
        };

        // Now publish an event for one of the *disconnected* metrics. If the
        // index still held the dead subscription, the manager would try to
        // send a Delta on resp_tx and we'd see it on resp_rx. We should not.
        let batch = MetricBatch {
            run_id,
            keys: vec![m1.clone()],
            points: vec![MetricPoint {
                key_index: 0,
                step: Step::new(1),
                value: 0.5,
                timestamp_ms: 0,
            }],
        };
        event_tx
            .send(PhotonEvent::BatchDecoded {
                run_id,
                batch: Arc::new(batch),
            })
            .unwrap();

        // Consume any messages for ~50ms; the disconnected `m1`/`m2` subs must
        // not fire. The surviving `m3` sub would only fire on an event for
        // `m3`, which we don't publish.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        while let Ok((id, update)) = resp_rx.try_recv() {
            if id == drain_id {
                panic!("surviving sub received an unrelated event: {update:?}");
            }
            panic!("disconnected sub leaked a message: ({id:?}, {update:?})");
        }

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn unsubscribe_cleans_up() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let (event_tx, _) = broadcast::channel(16);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        let manager = SubscriptionManager::new(buckets, metrics, tier_selector());
        let handle = tokio::spawn(manager.run(event_tx.subscribe(), cmd_rx));

        cmd_tx
            .send(ManagerCommand::Subscribe {
                query: MetricQuery {
                    run_id,
                    key: metric,
                    step_range: Step::ZERO..Step::MAX,
                    target_points: 500,
                },
                response_tx: resp_tx,
            })
            .unwrap();

        // Get subscription ID from snapshot
        let sub_id = match resp_rx.recv().await.unwrap() {
            (id, SubscriptionUpdate::Snapshot { .. }) => id,
            other => panic!("expected Snapshot, got {other:?}"),
        };

        cmd_tx.send(ManagerCommand::Unsubscribe(sub_id)).unwrap();

        let msg = resp_rx.recv().await.unwrap();
        assert!(matches!(msg, (id, SubscriptionUpdate::Unsubscribed) if id == sub_id));

        drop(cmd_tx);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auto_coarsen_triggers_resnapshot() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        // Width 2, budget 3 → coarsen triggers at > 4 raw points.
        // With 5 raw points, count/width = 5/2 = 2 < 3 → still Raw.
        // With 6 raw points, 6/2 = 3 >= 3 → Bucketed(0).
        // So coarsen fires after the 5th delta and picks Bucketed(0) once
        // we have 6+ points.
        let selector = TierSelector::new(vec![2]);
        let target_points = 3;

        // Pre-populate buckets so the resnapshot read has data
        let bucket_entries: Vec<BucketEntry> = (0..10)
            .map(|i| BucketEntry {
                run_id,
                key: metric.clone(),
                tier: 0,
                bucket: Bucket {
                    step_start: Step::new(i * 2),
                    step_end: Step::new(i * 2 + 1),
                    value: i as f64,
                    count: 2,
                    min: i as f64,
                    max: (i + 1) as f64,
                },
            })
            .collect();
        buckets.write_buckets(&bucket_entries).await.unwrap();

        let (event_tx, _) = broadcast::channel(64);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel();

        let manager = SubscriptionManager::new(buckets, metrics.clone(), selector);
        let handle = tokio::spawn(manager.run(event_tx.subscribe(), cmd_rx));

        cmd_tx
            .send(ManagerCommand::Subscribe {
                query: MetricQuery {
                    run_id,
                    key: metric.clone(),
                    step_range: Step::ZERO..Step::MAX,
                    target_points,
                },
                response_tx: resp_tx,
            })
            .unwrap();

        // Consume initial snapshot (Raw since 0 points in metrics store)
        let msg = resp_rx.recv().await.unwrap();
        assert!(matches!(msg, (_, SubscriptionUpdate::Snapshot { ref series })
            if matches!(series.data, SeriesData::Raw { .. })));

        // Send enough raw points to exceed 1.5x budget (> 4 points)
        // and enough that the planner picks Bucketed (>= 6 points).
        for step in 0..8u64 {
            let batch = MetricBatch {
                run_id,
                keys: vec![metric.clone()],
                points: vec![MetricPoint {
                    key_index: 0,
                    step: Step::new(step),
                    value: step as f64,
                    timestamp_ms: 0,
                }],
            };
            metrics.write_batch(&batch).await.unwrap();

            event_tx
                .send(PhotonEvent::BatchDecoded {
                    run_id,
                    batch: Arc::new(batch),
                })
                .unwrap();
        }

        // Collect messages: expect Deltas then a coarsened Snapshot (bucketed)
        let mut saw_coarsen = false;
        let mut delta_count = 0;
        while let Ok(msg) =
            tokio::time::timeout(std::time::Duration::from_millis(200), resp_rx.recv()).await
        {
            match msg.unwrap() {
                (_, SubscriptionUpdate::Delta(_)) => delta_count += 1,
                (_, SubscriptionUpdate::Snapshot { series }) => {
                    assert!(matches!(series.data, SeriesData::Bucketed { .. }));
                    saw_coarsen = true;
                    break;
                }
                other => panic!("unexpected message: {other:?}"),
            }
        }

        assert!(
            saw_coarsen,
            "expected coarsened Snapshot after exceeding 1.5x budget (got {delta_count} deltas)"
        );
        assert!(
            delta_count >= 5,
            "should have sent at least 5 deltas before coarsening"
        );

        drop(cmd_tx);
        handle.await.unwrap();
    }
}
