mod reducer;

use std::collections::HashMap;

use photon_core::types::bucket::BucketEntry;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch};

use self::reducer::Reducer;
use crate::domain::changeset::ChangeSet;
use crate::domain::projections::Projection;

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

/// Stateful downsampling keyed by `(run, metric)`.
pub struct DownsampleProjection {
    widths: Vec<u64>,
    reducers: HashMap<(RunId, Metric), Reducer>,
}

impl DownsampleProjection {
    pub fn new(config: DownsampleConfig) -> Self {
        Self {
            widths: config.widths,
            reducers: HashMap::new(),
        }
    }

    fn entries_for_batch(&mut self, run_id: RunId, batch: &MetricBatch) -> Vec<BucketEntry> {
        let mut entries = Vec::new();

        for point in &batch.points {
            let key = &batch.keys[point.key_index as usize];
            let reducer = self
                .reducers
                .entry((run_id, key.clone()))
                .or_insert_with(|| Reducer::new(self.widths.clone()));

            for (tier, bucket) in reducer.push(point.step, point.value) {
                entries.push(BucketEntry {
                    run_id,
                    key: key.clone(),
                    tier,
                    bucket,
                });
            }
        }

        entries
    }

    fn entries_for_run_finish(&mut self, run_id: RunId) -> Vec<BucketEntry> {
        let keys: Vec<_> = self
            .reducers
            .keys()
            .filter(|(id, _)| *id == run_id)
            .cloned()
            .collect();

        let mut entries = Vec::new();

        for (id, key) in keys {
            let Some(mut reducer) = self.reducers.remove(&(id, key.clone())) else {
                continue;
            };

            for (tier, bucket) in reducer.flush() {
                entries.push(BucketEntry {
                    run_id,
                    key: key.clone(),
                    tier,
                    bucket,
                });
            }
        }

        entries
    }
}

impl Projection for DownsampleProjection {
    fn project(&mut self, run_id: RunId, batch: &MetricBatch, changeset: &mut ChangeSet) {
        let entries = self.entries_for_batch(run_id, batch);
        if !entries.is_empty() {
            changeset.add_bucket_entries(entries);
        }
    }

    fn finish_run(&mut self, run_id: RunId, changeset: &mut ChangeSet) {
        let entries = self.entries_for_run_finish(run_id);
        if !entries.is_empty() {
            changeset.add_bucket_entries(entries);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use photon_core::types::event::PhotonEvent;
    use photon_core::types::metric::{MetricPoint, Step};

    fn apply_batch(projection: &mut DownsampleProjection, batch: &MetricBatch) -> ChangeSet {
        let mut changeset = ChangeSet::new();
        projection.project(batch.run_id, batch, &mut changeset);
        changeset
    }

    #[test]
    fn test_produces_buckets_at_step_boundary() {
        let mut projection = DownsampleProjection::new(DownsampleConfig { widths: vec![5] });
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

        let changeset = apply_batch(&mut projection, &batch);

        assert_eq!(changeset.bucket_entries.len(), 2);
        assert_eq!(changeset.bucket_entries[0].tier, 0);
        assert_eq!(changeset.bucket_entries[1].tier, 0);
    }

    #[test]
    fn test_partial_bucket_not_emitted() {
        let mut projection = DownsampleProjection::new(DownsampleConfig { widths: vec![10] });
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

        let changeset = apply_batch(&mut projection, &batch);
        assert!(changeset.bucket_entries.is_empty());
    }

    #[test]
    fn test_successive_applies_accumulate() {
        let mut projection = DownsampleProjection::new(DownsampleConfig { widths: vec![5] });
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

        let changeset = apply_batch(&mut projection, &batch);
        assert_eq!(changeset.bucket_entries.len(), 1);

        let second = apply_batch(
            &mut projection,
            &MetricBatch {
                run_id,
                keys: vec![Metric::new("train/loss").unwrap()],
                points: vec![],
            },
        );
        assert!(second.bucket_entries.is_empty());
    }

    #[test]
    fn test_multi_tier() {
        let mut projection = DownsampleProjection::new(DownsampleConfig {
            widths: vec![10, 30],
        });
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

        let changeset = apply_batch(&mut projection, &batch);

        let tier_0 = changeset
            .bucket_entries
            .iter()
            .filter(|entry| entry.tier == 0)
            .count();
        let tier_1 = changeset
            .bucket_entries
            .iter()
            .filter(|entry| entry.tier == 1)
            .count();
        assert_eq!(tier_0, 4);
        assert_eq!(tier_1, 1);
    }

    #[test]
    fn test_bucket_entries_have_events() {
        let mut projection = DownsampleProjection::new(DownsampleConfig { widths: vec![5] });
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

        let changeset = apply_batch(&mut projection, &batch);
        let bucket_events = changeset
            .events
            .iter()
            .filter(|event| matches!(event, PhotonEvent::BucketsReduced { .. }))
            .count();
        assert_eq!(bucket_events, changeset.bucket_entries.len());
    }

    #[test]
    fn test_finish_run_emits_tail_bucket() {
        let mut projection = DownsampleProjection::new(DownsampleConfig { widths: vec![10] });
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

        let mut changeset = apply_batch(&mut projection, &batch);
        assert!(changeset.bucket_entries.is_empty());

        projection.finish_run(run_id, &mut changeset);

        assert_eq!(changeset.bucket_entries.len(), 1);
        assert_eq!(changeset.bucket_entries[0].key, metric);
        assert_eq!(changeset.bucket_entries[0].bucket.step_start, Step::new(0));
        assert_eq!(changeset.bucket_entries[0].bucket.step_end, Step::new(2));
    }

    #[test]
    fn test_finish_run_evicts_series() {
        let mut projection = DownsampleProjection::new(DownsampleConfig { widths: vec![10] });
        let run_id = RunId::new();

        let batch = MetricBatch {
            run_id,
            keys: vec![Metric::new("train/loss").unwrap()],
            points: vec![MetricPoint {
                key_index: 0,
                value: 1.0,
                step: Step::new(0),
                timestamp_ms: 0,
            }],
        };

        let mut changeset = apply_batch(&mut projection, &batch);
        projection.finish_run(run_id, &mut changeset);

        let mut second = ChangeSet::new();
        projection.finish_run(run_id, &mut second);
        assert!(second.bucket_entries.is_empty());
    }
}
