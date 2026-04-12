use photon_core::types::bucket::Bucket;
use photon_core::types::metric::Step;

struct OpenBucket {
    width: u64,
    step_start: Step,
    step_end: Step,
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
}

impl OpenBucket {
    fn new(width: u64, window_start: Step, step: Step, value: f64) -> Self {
        Self {
            width,
            step_start: window_start,
            step_end: step,
            sum: value,
            count: 1,
            min: value,
            max: value,
        }
    }

    fn window_start(&self, step: Step) -> Step {
        Step::new((step.as_u64() / self.width) * self.width)
    }

    fn observe_point(&mut self, step: Step, value: f64) -> Option<Bucket> {
        let closed = self.advance_window(self.window_start(step));
        self.step_end = step;
        self.sum += value;
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        closed
    }

    fn observe_bucket(&mut self, bucket: &Bucket) -> Option<Bucket> {
        let closed = self.advance_window(self.window_start(bucket.step_start));
        self.step_end = bucket.step_end;
        self.sum += bucket.sum;
        self.count += bucket.count;
        self.min = self.min.min(bucket.min);
        self.max = self.max.max(bucket.max);
        closed
    }

    fn flush(&mut self) -> Bucket {
        let mean = if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        };
        Bucket {
            step_start: self.step_start,
            step_end: self.step_end,
            sum: self.sum,
            mean,
            count: self.count,
            min: self.min,
            max: self.max,
        }
    }

    fn advance_window(&mut self, next_window: Step) -> Option<Bucket> {
        if self.step_start == next_window {
            return None;
        }
        let closed = self.flush();
        self.step_start = next_window;
        self.step_end = next_window;
        self.sum = 0.0;
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        Some(closed)
    }
}

/// Multi-tier M4 reducer for a single (run, metric) stream.
///
/// Holds one [`OpenBucket`] per width. Tier 0 receives raw points,
/// higher tiers receive closed buckets from the tier below.
pub(super) struct Reducer {
    buckets: Vec<OpenBucket>,
    initialized: Vec<bool>,
}

impl Reducer {
    pub(super) fn new(widths: Vec<u64>) -> Self {
        assert!(!widths.is_empty(), "at least one tier width required");
        assert!(
            widths.windows(2).all(|pair| pair[0] < pair[1]),
            "widths must be sorted ascending",
        );
        let n = widths.len();
        Self {
            buckets: widths
                .into_iter()
                .map(|w| OpenBucket::new(w, Step::ZERO, Step::ZERO, 0.0))
                .collect(),
            initialized: vec![false; n],
        }
    }

    pub(super) fn push(&mut self, step: Step, value: f64) -> Vec<(usize, Bucket)> {
        let mut closed = Vec::new();

        let bucket = if self.initialized[0] {
            self.buckets[0].observe_point(step, value)
        } else {
            self.initialized[0] = true;
            let window = self.buckets[0].window_start(step);
            self.buckets[0] = OpenBucket::new(self.buckets[0].width, window, step, value);
            None
        };

        if let Some(b) = bucket {
            closed.push((0, b));
            self.promote_from(0, &mut closed);
        }

        closed
    }

    pub(super) fn flush(&mut self) -> Vec<(usize, Bucket)> {
        let mut closed = Vec::new();

        for i in 0..self.buckets.len() {
            if self.initialized[i] {
                let b = self.buckets[i].flush();
                closed.push((i, b));
                self.initialized[i] = false;

                // Promote into next tier before flushing it
                if i + 1 < self.buckets.len() {
                    let last = closed.len() - 1;
                    self.promote_single(last, &mut closed);
                }
            }
        }

        closed
    }

    fn promote_from(&mut self, _start_tier: usize, closed: &mut Vec<(usize, Bucket)>) {
        let mut idx = closed.len() - 1;
        while idx < closed.len() {
            self.promote_single(idx, closed);
            idx += 1;
        }
    }

    fn promote_single(&mut self, idx: usize, closed: &mut Vec<(usize, Bucket)>) {
        let (tier_idx, ref bucket) = closed[idx];
        let next = tier_idx + 1;
        if next >= self.buckets.len() {
            return;
        }

        let result = if self.initialized[next] {
            self.buckets[next].observe_bucket(bucket)
        } else {
            self.initialized[next] = true;
            let window = self.buckets[next].window_start(bucket.step_start);
            let width = self.buckets[next].width;
            self.buckets[next] = OpenBucket {
                width,
                step_start: window,
                step_end: bucket.step_end,
                sum: bucket.sum,
                count: bucket.count,
                min: bucket.min,
                max: bucket.max,
            };
            None
        };

        if let Some(b) = result {
            closed.push((next, b));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_step_width_bucket_closes_on_window_boundary() {
        let mut reducer = Reducer::new(vec![10]);

        let mut closed = Vec::new();
        for step in 0..10 {
            closed.extend(reducer.push(Step::new(step), step as f64));
        }
        assert!(closed.is_empty());

        closed.extend(reducer.push(Step::new(10), 10.0));
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].0, 0);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(9));
    }

    #[test]
    fn test_sparse_metrics_close_correctly() {
        let mut reducer = Reducer::new(vec![100]);

        reducer.push(Step::new(5), 1.0);
        reducer.push(Step::new(50), 2.0);
        reducer.push(Step::new(99), 3.0);

        let closed = reducer.push(Step::new(100), 4.0);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(99));
        assert_eq!(closed[0].1.min, 1.0);
        assert_eq!(closed[0].1.max, 3.0);
    }

    #[test]
    fn test_coarser_tier_merges_finer_buckets() {
        let mut reducer = Reducer::new(vec![10, 30]);

        let mut all_closed = Vec::new();
        for step in 0..41 {
            all_closed.extend(reducer.push(Step::new(step), step as f64));
        }

        let tier_0: Vec<_> = all_closed.iter().filter(|(t, _)| *t == 0).collect();
        let tier_1: Vec<_> = all_closed.iter().filter(|(t, _)| *t == 1).collect();

        assert_eq!(tier_0.len(), 4);
        assert_eq!(tier_1.len(), 1);
        assert_eq!(tier_1[0].1.step_start, Step::new(0));
    }

    #[test]
    fn test_coarser_tier_preserves_extrema() {
        let mut reducer = Reducer::new(vec![10, 30]);
        let mut all_closed = Vec::new();

        all_closed.extend(reducer.push(Step::new(0), 0.0));
        all_closed.extend(reducer.push(Step::new(5), 100.0));
        all_closed.extend(reducer.push(Step::new(9), 50.0));
        all_closed.extend(reducer.push(Step::new(10), 50.0));
        all_closed.extend(reducer.push(Step::new(15), -5.0));
        all_closed.extend(reducer.push(Step::new(19), 20.0));
        all_closed.extend(reducer.push(Step::new(20), 10.0));
        all_closed.extend(reducer.push(Step::new(29), 10.0));
        all_closed.extend(reducer.push(Step::new(30), 0.0));
        all_closed.extend(reducer.push(Step::new(40), 0.0));

        let tier_1: Vec<_> = all_closed.iter().filter(|(t, _)| *t == 1).collect();
        assert_eq!(tier_1.len(), 1);
        assert_eq!(tier_1[0].1.min, -5.0);
        assert_eq!(tier_1[0].1.max, 100.0);
    }

    #[test]
    fn test_flush_emits_partial_buckets() {
        let mut reducer = Reducer::new(vec![100]);
        reducer.push(Step::new(0), 1.0);
        reducer.push(Step::new(5), 2.0);

        let closed = reducer.flush();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(5));
    }

    #[test]
    fn test_flush_propagates_to_higher_tiers() {
        let mut reducer = Reducer::new(vec![10, 30]);

        // Push points 0-24, so tier-0 has closed [0,10), [10,20)
        // and tier-0 has open [20,24]. Tier-1 has open [0,?) with two child buckets.
        for step in 0..25 {
            reducer.push(Step::new(step), step as f64);
        }

        let closed = reducer.flush();

        let tier_0: Vec<_> = closed.iter().filter(|(t, _)| *t == 0).collect();
        let tier_1: Vec<_> = closed.iter().filter(|(t, _)| *t == 1).collect();

        assert_eq!(tier_0.len(), 1); // [20, 24]
        assert_eq!(tier_1.len(), 1); // [0, 24] — includes flushed tier-0
    }

    #[test]
    fn test_empty_reducer_flush_emits_nothing() {
        let mut reducer = Reducer::new(vec![10]);
        assert!(reducer.flush().is_empty());
    }

    #[test]
    fn test_large_step_gap_closes_bucket() {
        let mut reducer = Reducer::new(vec![10]);
        reducer.push(Step::new(0), 1.0);

        let closed = reducer.push(Step::new(1000), 2.0);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1.step_start, Step::new(0));
        assert_eq!(closed[0].1.step_end, Step::new(0));
    }
}
