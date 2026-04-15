use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, mpsc};

use photon_core::types::event::PhotonEvent;
use photon_core::types::id::{RunId, SubscriptionId};
use photon_core::types::metric::{Metric, Step};
use photon_core::types::query::{DataPoint, MetricQuery, MetricSeries, SeriesData};
use photon_core::types::resolution::{Resolution, TierSelector};
use photon_core::types::stream::SubscriptionUpdate;
use photon_store::ports::ReadError;
use photon_store::ports::bucket::BucketReader;
use photon_store::ports::metric::MetricReader;

use super::state::State;

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

#[derive(Clone)]
pub struct Service<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    state: Arc<Mutex<State>>,
    bucket_reader: B,
    metric_reader: M,
    tier_selector: TierSelector,
    next_id: Arc<AtomicU64>,
}

impl<B, M> Service<B, M>
where
    B: BucketReader,
    M: MetricReader,
{
    pub fn new(bucket_reader: B, metric_reader: M, tier_selector: TierSelector) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
            bucket_reader,
            metric_reader,
            tier_selector,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn alloc_id(&self) -> SubscriptionId {
        SubscriptionId::new(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Read a full series at the given resolution.
    async fn read_snapshot(
        &self,
        run_id: &RunId,
        key: &Metric,
        range: Range<Step>,
        resolution: &Resolution,
    ) -> Result<SeriesData, SubscriptionError> {
        match resolution {
            Resolution::Raw => {
                let points = self.metric_reader.read_points(run_id, key, range).await?;
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
                    .read_buckets(run_id, key, *tier, range)
                    .await?;
                Ok(SeriesData::Bucketed { buckets })
            }
        }
    }

    /// Re-pick resolution for a subscription that crossed the budget.
    /// Holds the state lock across async reads so events can't interleave
    /// and update `emitted_through` during the async window.
    async fn try_coarsen(&self, id: SubscriptionId) -> Result<(), SubscriptionError> {
        let mut state = self.state.lock().await;

        let Some(snapshot_args) = state.coarsen_args(id) else {
            return Ok(());
        };

        let total = self
            .metric_reader
            .count_points(
                &snapshot_args.run_id,
                &snapshot_args.metric,
                snapshot_args.range.clone(),
            )
            .await?;

        let new_resolution = self
            .tier_selector
            .pick(total, snapshot_args.target_points);
        if !new_resolution.is_coarser_than(&snapshot_args.current) {
            // No coarser tier qualifies yet. Push the threshold so we don't
            // re-query the store on every subsequent event.
            state.defer_coarsen(id);
            return Ok(());
        }

        let data = self
            .read_snapshot(
                &snapshot_args.run_id,
                &snapshot_args.metric,
                snapshot_args.range.clone(),
                &new_resolution,
            )
            .await?;

        let series = MetricSeries {
            run_id: snapshot_args.run_id,
            key: snapshot_args.metric.clone(),
            data,
        };

        state.replace_resolution(id, new_resolution, series);
        Ok(())
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

        // Hold the state lock across the async reads so no event can fire for
        // this (run_id, metric) between the snapshot-read and index-insert —
        // otherwise those events would be routed to a not-yet-indexed sub
        // and dropped.
        let mut state = self.state.lock().await;

        let point_count = self
            .metric_reader
            .count_points(&query.run_id, &query.key, query.step_range.clone())
            .await?;
        let resolution = self.tier_selector.pick(point_count, query.target_points);

        let data = self
            .read_snapshot(&query.run_id, &query.key, query.step_range.clone(), &resolution)
            .await?;

        let series = MetricSeries {
            run_id: query.run_id,
            key: query.key.clone(),
            data,
        };
        let _ = updates.send((id, SubscriptionUpdate::Snapshot { series: series.clone() }));

        state.register(id, &query, resolution, &series, updates);
        Ok(())
    }

    async fn unsubscribe(&self, id: SubscriptionId) {
        if let Some(sub) = self.state.lock().await.remove(id) {
            let _ = sub.response_tx.send((id, SubscriptionUpdate::Unsubscribed));
        }
    }

    async fn disconnect(&self, ids: Vec<SubscriptionId>) {
        let mut state = self.state.lock().await;
        for id in ids {
            // No notification, the receiving channel is already gone.
            let _ = state.remove(id);
        }
    }

    async fn handle_event(&self, event: PhotonEvent) -> Result<(), SubscriptionError> {
        let candidates = {
            let mut state = self.state.lock().await;
            match event {
                PhotonEvent::BatchDecoded { run_id, batch } => state.route_batch(run_id, &batch),
                PhotonEvent::BucketsReduced {
                    run_id,
                    key,
                    tier,
                    bucket,
                } => state.route_bucket(run_id, &key, tier, bucket),
                _ => return Ok(()),
            }
        };

        for id in candidates {
            self.try_coarsen(id).await?;
        }
        Ok(())
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

        service.subscribe(make_query(run_id, metric, 500), tx).await.unwrap();

        let msg = rx.recv().await.unwrap();
        match msg {
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
        let _ = rx.recv().await.unwrap();

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

        let msg = rx.recv().await.unwrap();
        match msg {
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
        let msg = rx.recv().await.unwrap();
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

        let msg = rx.recv().await.unwrap();
        match msg {
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
        let sub_id = match rx.recv().await.unwrap() {
            (id, SubscriptionUpdate::Snapshot { .. }) => id,
            other => panic!("expected Snapshot, got {other:?}"),
        };

        service.unsubscribe(sub_id).await;

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, (id, SubscriptionUpdate::Unsubscribed) if id == sub_id));
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

        service.subscribe(make_query(run_id, m1.clone(), 500), tx.clone()).await.unwrap();
        service.subscribe(make_query(run_id, m2, 500), tx.clone()).await.unwrap();

        let mut ids = Vec::new();
        for _ in 0..2 {
            match rx.recv().await.unwrap() {
                (id, SubscriptionUpdate::Snapshot { .. }) => ids.push(id),
                other => panic!("expected Snapshot, got {other:?}"),
            }
        }
        service.disconnect(ids).await;

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

        assert!(rx.try_recv().is_err(), "disconnected subs leaked a message");
    }

    #[tokio::test]
    async fn auto_coarsen_sends_replacement_snapshot() {
        let metrics = InMemoryMetricStore::new();
        let buckets = InMemoryBucketStore::new();

        let run_id = RunId::new();
        let metric = Metric::new("loss").unwrap();

        let selector = TierSelector::new(vec![2]);
        let target_points = 3;

        let bucket_entries: Vec<BucketEntry> = (0..10)
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

        let msg = rx.recv().await.unwrap();
        assert!(matches!(msg, (_, SubscriptionUpdate::Snapshot { ref series })
            if matches!(series.data, SeriesData::Raw { .. })));

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
        while let Ok(msg) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            match msg.unwrap() {
                (_, SubscriptionUpdate::DeltaPoints(_) | SubscriptionUpdate::DeltaBuckets(_)) => {
                    delta_count += 1;
                }
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
            "expected coarsened Snapshot after exceeding 1.5× budget (got {delta_count} deltas)"
        );
    }
}
