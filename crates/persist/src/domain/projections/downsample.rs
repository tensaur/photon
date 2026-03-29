use std::collections::HashMap;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};
use photon_downsample::ports::aggregator::Aggregator;
use photon_downsample::reducer::Reducer;

use super::Projection;

/// Streaming downsample projection.
///
/// Maintains a [`Reducer`] per (run, metric) pair. As points flow through,
/// closed buckets accumulate in a pending buffer. On [`drain`], the buffer
/// is returned so the service can write it in a single bulk INSERT.
pub struct DownsampleProjection<A: Aggregator> {
    aggregator: A,
    widths: Vec<u64>,
    reducers: HashMap<(RunId, Metric), Reducer<A>>,
    pending: HashMap<RunId, Vec<BucketEntry>>,
}

impl<A: Aggregator> DownsampleProjection<A> {
    pub fn new(aggregator: A, widths: Vec<u64>) -> Self {
        Self {
            aggregator,
            widths,
            reducers: HashMap::new(),
            pending: HashMap::new(),
        }
    }
}

impl<A: Aggregator> Projection for DownsampleProjection<A> {
    type Output = HashMap<RunId, Vec<BucketEntry>>;

    fn process(&mut self, run_id: RunId, batch: &MetricBatch) {
        for point in &batch.points {
            let key = &batch.keys[point.key_index as usize];
            let reducer = self
                .reducers
                .entry((run_id, key.clone()))
                .or_insert_with(|| Reducer::new(self.aggregator.clone(), self.widths.clone()));

            for (tier, bucket) in reducer.push(point.step, point.value) {
                self.pending.entry(run_id).or_default().push(BucketEntry {
                    key: key.clone(),
                    tier,
                    bucket,
                });
            }
        }
    }

    fn drain(&mut self) -> Self::Output {
        std::mem::take(&mut self.pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_core::types::metric::{Metric, MetricBatch, MetricPoint, Step};
    use photon_downsample::aggregator::m4::M4Aggregator;

    #[test]
    fn test_produces_buckets_at_step_boundary() {
        let mut proj = DownsampleProjection::new(M4Aggregator, vec![5]);
        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..11)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        proj.process(run_id, &batch);
        let by_run = proj.drain();
        let buckets = &by_run[&run_id];

        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].tier, 0);
        assert_eq!(buckets[1].tier, 0);
    }

    #[test]
    fn test_partial_bucket_not_emitted() {
        let mut proj = DownsampleProjection::new(M4Aggregator, vec![10]);
        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..3)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        proj.process(run_id, &batch);
        assert!(proj.drain().is_empty());
    }

    #[test]
    fn test_drain_clears_pending() {
        let mut proj = DownsampleProjection::new(M4Aggregator, vec![5]);
        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..6)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        proj.process(run_id, &batch);
        assert_eq!(proj.drain().values().map(|v| v.len()).sum::<usize>(), 1);
        assert!(proj.drain().is_empty());
    }

    #[test]
    fn test_multi_tier() {
        let mut proj = DownsampleProjection::new(M4Aggregator, vec![10, 30]);
        let run_id = RunId::new();
        let metric = Metric::new("train/loss").unwrap();

        let batch = MetricBatch {
            run_id,
            keys: vec![metric.clone()],
            points: (0..41)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        proj.process(run_id, &batch);
        let by_run = proj.drain();
        let buckets = &by_run[&run_id];

        let tier_0 = buckets.iter().filter(|e| e.tier == 0).count();
        let tier_1 = buckets.iter().filter(|e| e.tier == 1).count();
        assert_eq!(tier_0, 4);
        assert_eq!(tier_1, 1);
    }
}
