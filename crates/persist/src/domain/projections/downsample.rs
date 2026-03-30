use std::collections::HashMap;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};
use photon_downsample::ports::aggregator::Aggregator;
use photon_downsample::reducer::Reducer;

use super::Projection;
use crate::domain::changeset::ChangeSet;

#[derive(Clone)]
pub struct DownsampleConfig {
    pub widths: Vec<u64>,
}

impl Default for DownsampleConfig {
    fn default() -> Self {
        Self {
            widths: vec![100, 1000, 10000],
        }
    }
}

/// Streaming downsample projection.
pub struct DownsampleProjection<A: Aggregator> {
    aggregator: A,
    widths: Vec<u64>,
    reducers: HashMap<(RunId, Metric), Reducer<A>>,
}

impl<A: Aggregator> DownsampleProjection<A> {
    pub fn new(aggregator: A, config: DownsampleConfig) -> Self {
        Self {
            aggregator,
            widths: config.widths,
            reducers: HashMap::new(),
        }
    }
}

impl<A: Aggregator> Projection for DownsampleProjection<A> {
    fn apply(&mut self, run_id: RunId, batch: &MetricBatch, plan: &mut ChangeSet) {
        let mut entries = Vec::new();

        for point in &batch.points {
            let key = &batch.keys[point.key_index as usize];
            let reducer = self
                .reducers
                .entry((run_id, key.clone()))
                .or_insert_with(|| Reducer::new(self.aggregator.clone(), self.widths.clone()));

            for (tier, bucket) in reducer.push(point.step, point.value) {
                entries.push(BucketEntry {
                    run_id,
                    key: key.clone(),
                    tier,
                    bucket,
                });
            }
        }

        if !entries.is_empty() {
            plan.add_bucket_entries(entries);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::projections::ChangeSet;
    use photon_core::types::event::PhotonEvent;
    use photon_core::types::metric::{Metric, MetricBatch, MetricPoint, Step};
    use photon_downsample::aggregator::m4::M4Aggregator;

    fn apply_batch(proj: &mut DownsampleProjection<M4Aggregator>, batch: &MetricBatch) -> ChangeSet {
        let mut plan = ChangeSet::new();
        proj.apply(batch.run_id, batch, &mut plan);
        plan
    }

    #[test]
    fn test_produces_buckets_at_step_boundary() {
        let mut proj = DownsampleProjection::new(M4Aggregator, DownsampleConfig { widths: vec![5] });
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

        let plan = apply_batch(&mut proj, &batch);

        assert_eq!(plan.bucket_entries.len(), 2);
        assert_eq!(plan.bucket_entries[0].tier, 0);
        assert_eq!(plan.bucket_entries[1].tier, 0);
    }

    #[test]
    fn test_partial_bucket_not_emitted() {
        let mut proj = DownsampleProjection::new(M4Aggregator, DownsampleConfig { widths: vec![10] });
        let run_id = RunId::new();

        let batch = MetricBatch {
            run_id,
            keys: vec![Metric::new("train/loss").unwrap()],
            points: (0..3)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        let plan = apply_batch(&mut proj, &batch);
        assert!(plan.bucket_entries.is_empty());
    }

    #[test]
    fn test_successive_applies_accumulate() {
        let mut proj = DownsampleProjection::new(M4Aggregator, DownsampleConfig { widths: vec![5] });
        let run_id = RunId::new();

        let batch = MetricBatch {
            run_id,
            keys: vec![Metric::new("train/loss").unwrap()],
            points: (0..6)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        let plan = apply_batch(&mut proj, &batch);
        assert_eq!(plan.bucket_entries.len(), 1);

        // Second apply with no new closing points produces nothing
        let plan2 = apply_batch(&mut proj, &MetricBatch {
            run_id,
            keys: vec![Metric::new("train/loss").unwrap()],
            points: vec![],
        });
        assert!(plan2.bucket_entries.is_empty());
    }

    #[test]
    fn test_multi_tier() {
        let mut proj = DownsampleProjection::new(M4Aggregator, DownsampleConfig { widths: vec![10, 30] });
        let run_id = RunId::new();

        let batch = MetricBatch {
            run_id,
            keys: vec![Metric::new("train/loss").unwrap()],
            points: (0..41)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        let plan = apply_batch(&mut proj, &batch);

        let tier_0 = plan.bucket_entries.iter().filter(|e| e.tier == 0).count();
        let tier_1 = plan.bucket_entries.iter().filter(|e| e.tier == 1).count();
        assert_eq!(tier_0, 4);
        assert_eq!(tier_1, 1);
    }

    #[test]
    fn test_bucket_entries_have_events() {
        let mut proj = DownsampleProjection::new(M4Aggregator, DownsampleConfig { widths: vec![5] });
        let run_id = RunId::new();

        let batch = MetricBatch {
            run_id,
            keys: vec![Metric::new("train/loss").unwrap()],
            points: (0..11)
                .map(|i| MetricPoint {
                    key_index: 0,
                    value: i as f64,
                    step: Step::new(i),
                    timestamp_ms: i * 1000,
                })
                .collect(),
        };

        let plan = apply_batch(&mut proj, &batch);
        let bucket_events = plan.events.iter().filter(|e| {
            matches!(e, PhotonEvent::BucketsReduced { .. })
        }).count();
        assert_eq!(bucket_events, plan.bucket_entries.len());
    }
}
