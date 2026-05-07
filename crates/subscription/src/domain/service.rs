use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;

use photon_core::types::event::PhotonEvent;
use photon_core::types::id::SubscriptionId;
use photon_core::types::metric::Step;
use photon_core::types::query::{DataPoint, MetricQuery, MetricSeries, SeriesData};
use photon_core::types::resolution::{Resolution, TierSelector};
use photon_core::types::stream::SubscriptionUpdate;
use photon_store::ports::ReadError;
use photon_store::ports::bucket::BucketReader;
use photon_store::ports::metric::MetricReader;

use super::state::{RouterIndex, SubscriptionState};

/// Per-connection channel
pub type SubscriptionSender = mpsc::UnboundedSender<(SubscriptionId, SubscriptionUpdate)>;
pub type SubscriptionReceiver = mpsc::UnboundedReceiver<(SubscriptionId, SubscriptionUpdate)>;

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error("store read failed")]
    Read(#[from] ReadError),
}

pub trait SubscriptionService: Clone + Send + Sync + 'static {
    fn subscribe(
        &self,
        query: MetricQuery,
        updates: SubscriptionSender,
    ) -> impl Future<Output = Result<(), SubscriptionError>> + Send;

    fn unsubscribe(&self, id: SubscriptionId) -> impl Future<Output = ()> + Send;

    fn disconnect(&self, ids: Vec<SubscriptionId>) -> impl Future<Output = ()> + Send;

    fn handle_event(
        &self,
        event: PhotonEvent,
    ) -> impl Future<Output = Result<(), SubscriptionError>> + Send;
}

enum SubMsg {
    Event(PhotonEvent),
    /// Explicit unsubscribe — task sends a final `Unsubscribed` before exit.
    ShutdownUnsubscribe,
    /// WS connection closed — task exits without sending anything.
    ShutdownDisconnect,
}

/// Control handle held by the service for one subscription task.
struct SubHandle {
    msg_tx: mpsc::UnboundedSender<SubMsg>,
}

/// Per-subscription actor
#[derive(Clone)]
pub struct Service<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    subs: Arc<DashMap<SubscriptionId, SubHandle>>,
    indexes: RouterIndex,
    next_id: Arc<AtomicU64>,
    bucket_reader: B,
    metric_reader: M,
    tier_selector: TierSelector,
}

impl<B, M> Service<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    pub fn new(bucket_reader: B, metric_reader: M, tier_selector: TierSelector) -> Self {
        Self {
            subs: Arc::new(DashMap::new()),
            indexes: RouterIndex::default(),
            next_id: Arc::new(AtomicU64::new(1)),
            bucket_reader,
            metric_reader,
            tier_selector,
        }
    }

    fn alloc_id(&self) -> SubscriptionId {
        SubscriptionId::new(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

impl<B, M> SubscriptionService for Service<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    async fn subscribe(
        &self,
        query: MetricQuery,
        updates: SubscriptionSender,
    ) -> Result<(), SubscriptionError> {
        let id = self.alloc_id();

        // Pick the initial resolution based on the current point count.
        let point_count = self
            .metric_reader
            .count_points(&query.run_id, &query.key, query.step_range.clone())
            .await?;
        let resolution = self.tier_selector.pick(point_count, query.target_points);

        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        self.indexes.add(id, query.run_id, &query.key, &resolution);
        self.subs.insert(id, SubHandle { msg_tx });

        let ctx = SubContext {
            state: SubscriptionState::new(id, &query, resolution.clone(), updates),
            range: query.step_range,
            initial_resolution: resolution,
            msg_rx,
            bucket_reader: self.bucket_reader.clone(),
            metric_reader: self.metric_reader.clone(),
            tier_selector: self.tier_selector.clone(),
            indexes: self.indexes.clone(),
        };
        tokio::spawn(run_subscription(ctx));

        Ok(())
    }

    async fn unsubscribe(&self, id: SubscriptionId) {
        if let Some((_, handle)) = self.subs.remove(&id) {
            let _ = handle.msg_tx.send(SubMsg::ShutdownUnsubscribe);
        }
    }

    async fn disconnect(&self, ids: Vec<SubscriptionId>) {
        for id in ids {
            if let Some((_, handle)) = self.subs.remove(&id) {
                let _ = handle.msg_tx.send(SubMsg::ShutdownDisconnect);
            }
        }
    }

    async fn handle_event(&self, event: PhotonEvent) -> Result<(), SubscriptionError> {
        match &event {
            PhotonEvent::BatchDecoded { run_id, batch } => {
                for metric in &batch.keys {
                    let Some(ids) = self.indexes.raw_subs(*run_id, metric) else {
                        continue;
                    };
                    for id in ids {
                        if let Some(handle) = self.subs.get(&id) {
                            let _ = handle.msg_tx.send(SubMsg::Event(event.clone()));
                        }
                    }
                }
            }
            PhotonEvent::BucketsReduced {
                run_id, key, tier, ..
            } => {
                let Some(ids) = self.indexes.bucket_subs(*run_id, key, *tier) else {
                    return Ok(());
                };
                for id in ids {
                    if let Some(handle) = self.subs.get(&id) {
                        let _ = handle.msg_tx.send(SubMsg::Event(event.clone()));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

struct SubContext<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    state: SubscriptionState,
    range: Range<Step>,
    initial_resolution: Resolution,
    msg_rx: mpsc::UnboundedReceiver<SubMsg>,
    bucket_reader: B,
    metric_reader: M,
    tier_selector: TierSelector,
    indexes: RouterIndex,
}

async fn run_subscription<B, M>(mut ctx: SubContext<B, M>)
where
    B: BucketReader,
    M: MetricReader,
{
    // Step 1: read the initial snapshot and apply it. Events that fire
    // during this read are already queued in `msg_rx` (index registered
    // before spawn) — they'll be processed by the main loop and filtered by
    // `emitted_through`.
    if let Err(e) = ctx.initialize().await {
        tracing::error!("subscribe initialisation failed: {e}");
        ctx.unregister();
        return;
    }

    // Step 2: main event loop. FIFO on `msg_rx` guarantees shutdown arrives
    // after any events sent before it.
    while let Some(msg) = ctx.msg_rx.recv().await {
        match msg {
            SubMsg::Event(event) => {
                if let Err(e) = ctx.process(event).await {
                    tracing::error!("subscription event processing failed: {e}");
                }
            }
            SubMsg::ShutdownUnsubscribe => {
                ctx.state.send_unsubscribed();
                break;
            }
            SubMsg::ShutdownDisconnect => break,
        }
    }

    ctx.unregister();
}

impl<B, M> SubContext<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    async fn initialize(&mut self) -> Result<(), SubscriptionError> {
        let data = self
            .read_snapshot(&self.initial_resolution.clone(), self.range.clone())
            .await?;
        let series = MetricSeries {
            run_id: self.state.run_id,
            key: self.state.metric.clone(),
            data,
        };
        self.state.send_snapshot(series.clone());
        self.state.apply_snapshot(&series.data);
        Ok(())
    }

    async fn read_snapshot(
        &self,
        resolution: &Resolution,
        range: Range<Step>,
    ) -> Result<SeriesData, SubscriptionError> {
        match resolution {
            Resolution::Raw => {
                let points = self
                    .metric_reader
                    .read_points(&self.state.run_id, &self.state.metric, range)
                    .await?;
                Ok(SeriesData::Raw {
                    points: points
                        .into_iter()
                        .map(|(step, value)| DataPoint { step, value })
                        .collect(),
                })
            }
            Resolution::Bucketed(tier) => {
                let buckets = self
                    .bucket_reader
                    .read_buckets(&self.state.run_id, &self.state.metric, *tier, range)
                    .await?;
                Ok(SeriesData::Bucketed { buckets })
            }
        }
    }

    async fn process(&mut self, event: PhotonEvent) -> Result<(), SubscriptionError> {
        let triggered = match event {
            PhotonEvent::BatchDecoded { run_id, batch } => {
                self.state.apply_batch(run_id, &batch)
            }
            PhotonEvent::BucketsReduced {
                run_id,
                key,
                tier,
                bucket,
            } => self.state.apply_bucket(run_id, &key, tier, bucket),
            _ => false,
        };

        if triggered {
            self.try_coarsen().await?;
        }
        Ok(())
    }

    /// Re-pick resolution. If a coarser tier qualifies, read a fresh snapshot
    /// at that tier and replace this subscription's indexing. Runs entirely
    /// inside this task — blocks only this subscription, not any others.
    async fn try_coarsen(&mut self) -> Result<(), SubscriptionError> {
        // Half-open range: include the last emitted step so the snapshot covers
        // everything raw mode already showed, without dropping a bucket-width
        // of recent coverage.
        let coarsen_end = Step::new(self.state.emitted_through.as_u64().saturating_add(1));
        let coarsen_range = self.range.start..coarsen_end;
        let total = self
            .metric_reader
            .count_points(&self.state.run_id, &self.state.metric, coarsen_range.clone())
            .await?;

        let new_resolution = self.tier_selector.pick(total, self.state.target_points);
        if !new_resolution.is_coarser_than(&self.state.current_resolution) {
            self.state.defer_coarsen();
            return Ok(());
        }

        let data = self.read_snapshot(&new_resolution, coarsen_range).await?;
        let series = MetricSeries {
            run_id: self.state.run_id,
            key: self.state.metric.clone(),
            data,
        };
        self.state.send_snapshot(series.clone());

        let old_resolution =
            std::mem::replace(&mut self.state.current_resolution, new_resolution.clone());
        self.state.apply_snapshot(&series.data);

        self.indexes.remove(
            self.state.id,
            self.state.run_id,
            &self.state.metric,
            &old_resolution,
        );
        self.indexes.add(
            self.state.id,
            self.state.run_id,
            &self.state.metric,
            &new_resolution,
        );
        Ok(())
    }

    fn unregister(&self) {
        self.indexes.remove(
            self.state.id,
            self.state.run_id,
            &self.state.metric,
            &self.state.current_resolution,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use photon_core::types::bucket::{Bucket, BucketEntry};
    use photon_core::types::id::RunId;
    use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};
    use photon_store::memory::bucket::InMemoryBucketStore;
    use photon_store::memory::metric::InMemoryMetricStore;
    use photon_store::ports::bucket::BucketWriter;
    use photon_store::ports::metric::MetricWriter;

    use super::*;

    fn tier_selector() -> TierSelector {
        TierSelector::new(vec![5])
    }

    fn make_query(run_id: RunId, metric: Metric, target_points: usize) -> MetricQuery {
        MetricQuery {
            run_id,
            key: metric,
            step_range: Step::ZERO..Step::MAX,
            target_points,
        }
    }

    /// Poll the receiver until a message arrives or the timeout elapses.
    /// Needed because per-sub tasks are async, so tests have to wait for the
    /// spawned task to produce output.
    async fn recv(rx: &mut SubscriptionReceiver) -> (SubscriptionId, SubscriptionUpdate) {
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timed out waiting for update")
            .expect("channel closed")
    }

    #[tokio::test]
    async fn subscribe_sends_raw_snapshot() {
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

        let service = Service::new(buckets, metrics, tier_selector());
        let (tx, mut rx) = mpsc::unbounded_channel();

        service
            .subscribe(make_query(run_id, metric, 500), tx)
            .await
            .unwrap();

        match recv(&mut rx).await {
            (_, SubscriptionUpdate::Snapshot { series }) => {
                assert!(matches!(series.data, SeriesData::Raw { ref points } if points.len() == 3));
            }
            other => panic!("expected Snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn raw_delta_after_subscribe() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        let service = Service::new(buckets, metrics, tier_selector());
        let (tx, mut rx) = mpsc::unbounded_channel();

        service
            .subscribe(make_query(run_id, metric.clone(), 500), tx)
            .await
            .unwrap();
        let _ = recv(&mut rx).await; // consume snapshot

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
        service
            .handle_event(PhotonEvent::BatchDecoded {
                run_id,
                batch: Arc::new(batch),
            })
            .await
            .unwrap();

        match recv(&mut rx).await {
            (_, SubscriptionUpdate::DeltaPoints(points)) => {
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].step, Step::new(10));
            }
            other => panic!("expected DeltaPoints, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bucketed_delta_after_subscribe() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

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

        let bucket_entries: Vec<BucketEntry> = (0..1000)
            .map(|i| BucketEntry {
                run_id,
                key: metric.clone(),
                tier: 0,
                bucket: Bucket {
                    step_start: Step::new(i * 5),
                    step_end: Step::new(i * 5 + 4),
                    sum: (i * 5) as f64 * 5.0,
                    mean: (i * 5) as f64,
                    count: 5,
                    min: (i * 5) as f64,
                    max: (i * 5 + 4) as f64,
                },
            })
            .collect();
        buckets.write_buckets(&bucket_entries).await.unwrap();

        let service = Service::new(buckets, metrics, tier_selector());
        let (tx, mut rx) = mpsc::unbounded_channel();

        service
            .subscribe(make_query(run_id, metric.clone(), 500), tx)
            .await
            .unwrap();
        let msg = recv(&mut rx).await;
        assert!(matches!(msg, (_, SubscriptionUpdate::Snapshot { ref series })
            if matches!(series.data, SeriesData::Bucketed { ref buckets } if buckets.len() == 1000)));

        let new_bucket = Bucket {
            step_start: Step::new(5000),
            step_end: Step::new(5004),
            sum: 25010.0,
            mean: 5002.0,
            count: 5,
            min: 5000.0,
            max: 5004.0,
        };
        service
            .handle_event(PhotonEvent::BucketsReduced {
                run_id,
                key: metric,
                tier: 0,
                bucket: new_bucket.clone(),
            })
            .await
            .unwrap();

        match recv(&mut rx).await {
            (_, SubscriptionUpdate::DeltaBuckets(b)) => {
                assert_eq!(b.len(), 1);
                assert_eq!(b[0], new_bucket);
            }
            other => panic!("expected DeltaBuckets, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unsubscribe_notifies_and_cleans_up() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        let service = Service::new(buckets, metrics, tier_selector());
        let (tx, mut rx) = mpsc::unbounded_channel();

        service
            .subscribe(make_query(run_id, metric, 500), tx)
            .await
            .unwrap();
        let sub_id = match recv(&mut rx).await {
            (id, SubscriptionUpdate::Snapshot { .. }) => id,
            other => panic!("expected Snapshot, got {other:?}"),
        };

        service.unsubscribe(sub_id).await;

        match recv(&mut rx).await {
            (id, SubscriptionUpdate::Unsubscribed) => assert_eq!(id, sub_id),
            other => panic!("expected Unsubscribed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn disconnect_drops_all_subs_and_stops_routing() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let m1 = Metric::new("loss").unwrap();
        let m2 = Metric::new("acc").unwrap();

        let service = Service::new(buckets, metrics, tier_selector());
        let (tx, mut rx) = mpsc::unbounded_channel();

        service
            .subscribe(make_query(run_id, m1.clone(), 500), tx.clone())
            .await
            .unwrap();
        service
            .subscribe(make_query(run_id, m2, 500), tx.clone())
            .await
            .unwrap();

        let mut ids = Vec::new();
        for _ in 0..2 {
            match recv(&mut rx).await {
                (id, SubscriptionUpdate::Snapshot { .. }) => ids.push(id),
                other => panic!("expected Snapshot, got {other:?}"),
            }
        }
        service.disconnect(ids).await;

        // Give the tasks a moment to tear down.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let batch = MetricBatch {
            run_id,
            keys: vec![m1],
            points: vec![MetricPoint {
                key_index: 0,
                step: Step::new(1),
                value: 0.5,
                timestamp_ms: 0,
            }],
        };
        service
            .handle_event(PhotonEvent::BatchDecoded {
                run_id,
                batch: Arc::new(batch),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "disconnected subs leaked a message");
    }

    #[tokio::test]
    async fn auto_coarsen_sends_replacement_snapshot() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        // target_points=3 with width=2. Tier 0 qualifies when point count
        // ≥ target * width = 6. Initial threshold is 1.5*3 = 4, so coarsen
        // fires at sample=5 but fails (5/2=2 < 3). Exponential backoff
        // pushes threshold to 10. We need sample >10 to retry, at which point
        // tier 0 qualifies (10/2=5, still < 3... need sample ≥ 30 for buckets
        // ≥ target * 10 on backoff). Send enough points to cross.
        let selector = TierSelector::new(vec![2]);
        let target_points = 3;

        let bucket_entries: Vec<BucketEntry> = (0..30)
            .map(|i| BucketEntry {
                run_id,
                key: metric.clone(),
                tier: 0,
                bucket: Bucket {
                    step_start: Step::new(i * 2),
                    step_end: Step::new(i * 2 + 1),
                    sum: i as f64 * 2.0,
                    mean: i as f64,
                    count: 2,
                    min: i as f64,
                    max: (i + 1) as f64,
                },
            })
            .collect();
        buckets.write_buckets(&bucket_entries).await.unwrap();

        let service = Service::new(buckets, metrics.clone(), selector);
        let (tx, mut rx) = mpsc::unbounded_channel();

        service
            .subscribe(
                MetricQuery {
                    run_id,
                    key: metric.clone(),
                    step_range: Step::ZERO..Step::MAX,
                    target_points,
                },
                tx,
            )
            .await
            .unwrap();

        let msg = recv(&mut rx).await;
        assert!(matches!(msg, (_, SubscriptionUpdate::Snapshot { ref series })
            if matches!(series.data, SeriesData::Raw { .. })));

        for step in 0..60u64 {
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
            service
                .handle_event(PhotonEvent::BatchDecoded {
                    run_id,
                    batch: Arc::new(batch),
                })
                .await
                .unwrap();
        }

        let mut saw_coarsen = false;
        let mut delta_count = 0;
        loop {
            let msg = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await;
            match msg {
                Ok(Some((_, SubscriptionUpdate::DeltaPoints(_))))
                | Ok(Some((_, SubscriptionUpdate::DeltaBuckets(_)))) => delta_count += 1,
                Ok(Some((_, SubscriptionUpdate::Snapshot { series }))) => {
                    assert!(matches!(series.data, SeriesData::Bucketed { .. }));
                    saw_coarsen = true;
                    break;
                }
                Ok(Some(other)) => panic!("unexpected message: {other:?}"),
                Ok(None) | Err(_) => break,
            }
        }

        assert!(
            saw_coarsen,
            "expected coarsened Snapshot after exceeding 1.5× budget (got {delta_count} deltas)"
        );
    }
}
