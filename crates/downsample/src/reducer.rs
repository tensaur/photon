use photon_core::types::bucket::Bucket;

use crate::ports::aggregator::Aggregator;

pub struct Reducer<A: Aggregator> {
    aggregator: A,
    tiers: Vec<Tier<A>>,
}

struct Tier<A: Aggregator> {
    divisor: usize,
    count: usize,
    open: Option<A::Bucket>,
    first_step: u64,
    last_step: u64,
}

impl<A: Aggregator> Reducer<A> {
    pub fn new(aggregator: A, divisors: Vec<usize>) -> Self {
        let tiers = divisors
            .into_iter()
            .map(|divisor| Tier {
                divisor,
                count: 0,
                open: None,
                first_step: 0,
                last_step: 0,
            })
            .collect();

        Self { aggregator, tiers }
    }

    pub fn push(&mut self, step: u64, value: f64) -> Vec<(usize, Bucket)> {
        let mut closed = Vec::new();

        for (i, tier) in self.tiers.iter_mut().enumerate() {
            match &mut tier.open {
                Some(bucket) => {
                    self.aggregator.push(bucket, step, value);
                }
                None => {
                    tier.open = Some(self.aggregator.new_bucket(step, value));
                    tier.first_step = step;
                }
            }

            tier.last_step = step;
            tier.count += 1;

            if tier.count >= tier.divisor {
                let bucket = tier.open.take().unwrap();
                closed.push((
                    i,
                    self.aggregator
                        .close(&bucket, tier.first_step, tier.last_step),
                ));
                tier.count = 0;
            }
        }

        closed
    }

    /// Flush any partially-filled buckets. Call when a run finishes.
    pub fn flush(&mut self) -> Vec<(usize, Bucket)> {
        let mut closed = Vec::new();

        for (i, tier) in self.tiers.iter_mut().enumerate() {
            if let Some(bucket) = tier.open.take() {
                closed.push((
                    i,
                    self.aggregator
                        .close(&bucket, tier.first_step, tier.last_step),
                ));
                tier.count = 0;
            }
        }

        closed
    }
}
