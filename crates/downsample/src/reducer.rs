use photon_core::types::bucket::Bucket;
use photon_core::types::metric::Step;

use crate::ports::aggregator::Aggregator;

/// Multi-tier streaming reducer using step-width buckets.
///
/// Each tier defines a step width. Tier 0 aggregates raw points into
/// buckets of that width. Coarser tiers aggregate closed buckets from
/// the tier below via [`Aggregator::merge`], not raw points.
///
/// Buckets close when the stream advances into the next window.
pub struct Reducer<A: Aggregator> {
    aggregator: A,
    tiers: Vec<Tier<A>>,
}

struct Tier<A: Aggregator> {
    width: u64,
    open: Option<A::Bucket>,
    step_start: Step,
    step_end: Step,
    non_empty: bool,
}

impl<A: Aggregator> Tier<A> {
    fn window_start(&self, step: Step) -> Step {
        Step::new((step.as_u64() / self.width) * self.width)
    }
}

impl<A: Aggregator> Reducer<A> {
    pub fn new(aggregator: A, widths: Vec<u64>) -> Self {
        assert!(!widths.is_empty(), "at least one tier width required");
        let tiers = widths
            .into_iter()
            .map(|width| Tier {
                width,
                open: None,
                step_start: Step::ZERO,
                step_end: Step::ZERO,
                non_empty: false,
            })
            .collect();

        Self { aggregator, tiers }
    }

    /// Push a raw point. Returns any buckets that closed across all tiers.
    pub fn push(&mut self, step: Step, value: f64) -> Vec<(usize, Bucket)> {
        let mut closed = Vec::new();

        // Tier 0: aggregate raw points
        self.push_to_tier(0, step, value, &mut closed);

        // Coarser tiers: merge closed buckets from the tier below.
        // Process newly closed buckets and feed them up the tier chain.
        let mut i = 0;
        while i < closed.len() {
            let (tier_idx, bucket) = closed[i].clone();
            let next_tier = tier_idx + 1;
            if next_tier < self.tiers.len() {
                self.merge_into_tier(next_tier, &bucket, &mut closed);
            }
            i += 1;
        }

        closed
    }

    /// Flush all partially-filled buckets across all tiers.
    /// Call when a run finishes.
    pub fn flush(&mut self) -> Vec<(usize, Bucket)> {
        let mut closed = Vec::new();

        for (i, tier) in self.tiers.iter_mut().enumerate() {
            if let Some(bucket) = tier.open.take() {
                if tier.non_empty {
                    closed.push((
                        i,
                        self.aggregator
                            .close(&bucket, tier.step_start, tier.step_end),
                    ));
                }
                tier.non_empty = false;
            }
        }

        closed
    }

    fn push_to_tier(
        &mut self,
        tier_idx: usize,
        step: Step,
        value: f64,
        closed: &mut Vec<(usize, Bucket)>,
    ) {
        let tier = &mut self.tiers[tier_idx];
        let window = tier.window_start(step);

        // Close the current bucket if the step moved into a new window
        if tier.open.is_some() && window != tier.step_start {
            let bucket = tier.open.take().unwrap();
            if tier.non_empty {
                closed.push((
                    tier_idx,
                    self.aggregator
                        .close(&bucket, tier.step_start, tier.step_end),
                ));
            }
            let tier = &mut self.tiers[tier_idx];
            tier.open = None;
            tier.non_empty = false;
        }

        let tier = &mut self.tiers[tier_idx];
        if let Some(bucket) = &mut tier.open {
            self.aggregator.push(bucket, step, value);
        } else {
            tier.open = Some(self.aggregator.new_bucket(step, value));
            tier.step_start = window;
        }
        tier.step_end = step;
        tier.non_empty = true;
    }

    fn merge_into_tier(
        &mut self,
        tier_idx: usize,
        source: &Bucket,
        closed: &mut Vec<(usize, Bucket)>,
    ) {
        let tier = &mut self.tiers[tier_idx];
        let window = tier.window_start(source.step_start);

        // Close if we moved into a new window
        if tier.open.is_some() && window != tier.step_start {
            let bucket = tier.open.take().unwrap();
            if tier.non_empty {
                closed.push((
                    tier_idx,
                    self.aggregator
                        .close(&bucket, tier.step_start, tier.step_end),
                ));
            }
            let tier = &mut self.tiers[tier_idx];
            tier.open = None;
            tier.non_empty = false;
        }

        // Merge the source bucket into this tier
        let tier = &mut self.tiers[tier_idx];
        let source_bucket = self.aggregator.new_bucket(source.step_start, source.value);
        if let Some(existing) = &tier.open {
            tier.open = Some(self.aggregator.merge(existing, &source_bucket));
        } else {
            tier.open = Some(source_bucket);
            tier.step_start = window;
        }
        tier.step_end = source.step_end;
        tier.non_empty = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::m4::M4Aggregator;

    #[test]
    fn test_step_width_bucket_closes_on_window_boundary() {
        let mut reducer = Reducer::new(M4Aggregator, vec![10]);

        let mut closed = Vec::new();
        for step in 0..10 {
            closed.extend(reducer.push(Step::new(step), step as f64));
        }
        assert!(closed.is_empty(), "bucket should not close within window");

        // Step 10 opens window [10, 20), closing [0, 10)
        closed.extend(reducer.push(Step::new(10), 10.0));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].0, 0); // tier 0
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(9));
    }

    #[test]
    fn test_sparse_metrics_close_correctly() {
        let mut reducer = Reducer::new(M4Aggregator, vec![100]);

        // Only 3 points in a 100-step window
        reducer.push(Step::new(5), 1.0);
        reducer.push(Step::new(50), 2.0);
        reducer.push(Step::new(99), 3.0);

        // Crossing into next window closes the bucket
        let closed = reducer.push(Step::new(100), 4.0);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(99));
        assert_eq!(closed[0].1.min, 1.0);
        assert_eq!(closed[0].1.max, 3.0);
    }

    #[test]
    fn test_coarser_tier_merges_finer_buckets() {
        // Tier 0: width 10, Tier 1: width 30
        let mut reducer = Reducer::new(M4Aggregator, vec![10, 30]);

        let mut all_closed = Vec::new();
        // Steps 0-39: tier-0 closes [0,10), [10,20), [20,30) as they pass.
        // When [30,40) closes at step 40, tier-1 sees a bucket from window [30,60)
        // which closes the tier-1 [0,30) window.
        for step in 0..41 {
            all_closed.extend(reducer.push(Step::new(step), step as f64));
        }

        let tier_0: Vec<_> = all_closed.iter().filter(|(t, _)| *t == 0).collect();
        let tier_1: Vec<_> = all_closed.iter().filter(|(t, _)| *t == 1).collect();

        // 4 tier-0 buckets: [0,10), [10,20), [20,30), [30,40)
        assert_eq!(tier_0.len(), 4);
        // 1 tier-1 bucket: [0,30) closed when [30,40) arrived
        assert_eq!(tier_1.len(), 1);
        assert_eq!(tier_1[0].1.step_start, Step::new(0));
    }

    #[test]
    fn test_flush_emits_partial_buckets() {
        let mut reducer = Reducer::new(M4Aggregator, vec![100]);

        reducer.push(Step::new(0), 1.0);
        reducer.push(Step::new(5), 2.0);

        let closed = reducer.flush();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(5));
    }

    #[test]
    fn test_empty_reducer_flush_emits_nothing() {
        let mut reducer = Reducer::new(M4Aggregator, vec![10]);
        assert!(reducer.flush().is_empty());
    }

    #[test]
    fn test_large_step_gap_closes_bucket() {
        let mut reducer = Reducer::new(M4Aggregator, vec![10]);

        reducer.push(Step::new(0), 1.0);

        // Jump far ahead — closes the [0, 10) bucket
        let closed = reducer.push(Step::new(1000), 2.0);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(0));
    }
}
