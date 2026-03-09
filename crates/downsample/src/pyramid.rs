use photon_core::types::bucket::Bucket;

use crate::ports::aggregator::Aggregator;

/// Manages the resolution pyramid for a single metric key.
pub(crate) struct Pyramid<A: Aggregator> {
    aggregator: A,
    tiers: Vec<Tier<A>>,
}

struct Tier<A: Aggregator> {
    width: u64,
    open: Option<A::Bucket>,
}

impl<A: Aggregator> Pyramid<A> {
    pub fn new(aggregator: A, tier_widths: Vec<u64>) -> Self {
        let tiers = tier_widths
            .into_iter()
            .map(|width| Tier { width, open: None })
            .collect();

        Self { aggregator, tiers }
    }

    /// Push a single point. Returns a `(tier_width, Bucket)` for every
    /// tier boundary crossed.
    pub fn push(&mut self, step: u64, value: f64) -> Vec<(u64, Bucket)> {
        // Push into finest tier.
        match &mut self.tiers[0].open {
            Some(bucket) => self.aggregator.push(bucket, step, value),
            None => self.tiers[0].open = Some(self.aggregator.new_bucket(step, value)),
        }

        // Walk up the pyramid closing every tier that hit its boundary.
        let mut closed = Vec::new();

        for i in 0..self.tiers.len() {
            if !self.at_boundary(i, step) {
                break;
            }

            let inner = self.tiers[i].open.take().unwrap();

            closed.push((
                self.tiers[i].width,
                self.aggregator.close(&inner, self.range_start(i, step)),
            ));

            if i + 1 < self.tiers.len() {
                self.merge_into(i + 1, inner);
            }
        }

        closed
    }

    /// Merge a closed bucket's contents into a tier's open bucket.
    fn merge_into(&mut self, tier_index: usize, closed: A::Bucket) {
        let tier = &mut self.tiers[tier_index];
        match &mut tier.open {
            Some(existing) => *existing = self.aggregator.merge(existing, &closed),
            None => tier.open = Some(closed),
        }
    }

    /// Whether `step` is the last step in a bucket for the given tier.
    /// For width 100: true at step 99, 199, 299, ...
    fn at_boundary(&self, tier_index: usize, step: u64) -> bool {
        let width = self.tiers[tier_index].width;
        (step + 1).is_multiple_of(width)
    }

    /// The first step of the bucket that contains `step`.
    /// For width 100: step 0 for steps 0..99, step 100 for steps 100..199, ...
    fn range_start(&self, tier_index: usize, step: u64) -> u64 {
        let width = self.tiers[tier_index].width;
        step - (step % width)
    }
}
