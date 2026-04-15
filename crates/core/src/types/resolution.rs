use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resolution {
    Raw,
    Bucketed(usize),
}

impl Resolution {
    /// Returns true if `self` is a coarser resolution than `other`.
    pub fn is_coarser_than(&self, other: &Resolution) -> bool {
        match (self, other) {
            (Resolution::Raw, _) => false,
            (Resolution::Bucketed(_), Resolution::Raw) => true,
            (Resolution::Bucketed(a), Resolution::Bucketed(b)) => a > b,
        }
    }
}

/// Picks the resolution tier that best fits the target point budget.
#[derive(Clone)]
pub struct TierSelector {
    widths: Vec<u64>,
}

impl Default for TierSelector {
    fn default() -> Self {
        Self::new(vec![100, 1000, 10000])
    }
}

impl TierSelector {
    pub fn new(widths: Vec<u64>) -> Self {
        debug_assert!(
            widths.windows(2).all(|w| w[0] < w[1]),
            "widths must be sorted ascending"
        );
        Self { widths }
    }

    /// Pick the tier whose bucket count is closest to `target_points` from above.
    pub fn pick(&self, point_count: usize, target_points: usize) -> Resolution {
        if target_points == 0 {
            return Resolution::Raw;
        }

        let mut best: Option<(usize, usize)> = None; // (tier_index, bucket_count)

        for (i, w) in self.widths.iter().enumerate() {
            let buckets = point_count / *w as usize;
            if buckets >= target_points {
                match best {
                    None => best = Some((i, buckets)),
                    Some((_, prev)) if buckets < prev => best = Some((i, buckets)),
                    _ => {}
                }
            }
        }

        best.map_or(Resolution::Raw, |(i, _)| Resolution::Bucketed(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_when_few_points() {
        let selector = TierSelector::new(vec![100, 1000]);
        assert_eq!(selector.pick(50, 200), Resolution::Raw);
    }

    #[test]
    fn raw_when_no_tier_meets_budget() {
        let selector = TierSelector::new(vec![100, 1000]);
        assert_eq!(selector.pick(5_000, 200), Resolution::Raw);
    }

    #[test]
    fn picks_closest_fit_above_budget() {
        let selector = TierSelector::new(vec![100, 1000]);
        assert_eq!(selector.pick(200_000, 500), Resolution::Bucketed(0));
    }

    #[test]
    fn prefers_tighter_fit() {
        let selector = TierSelector::new(vec![100, 1000]);
        assert_eq!(selector.pick(200_000, 150), Resolution::Bucketed(1));
    }

    #[test]
    fn falls_back_to_finer_tier() {
        let selector = TierSelector::default(); // [100, 1000, 10000]
        assert_eq!(selector.pick(20_000, 100), Resolution::Bucketed(0));
    }

    #[test]
    fn coarsest_tier_for_large_dataset() {
        let selector = TierSelector::default(); // [100, 1000, 10000]
        assert_eq!(selector.pick(10_000_000, 500), Resolution::Bucketed(2));
    }

    #[test]
    fn raw_when_zero_budget() {
        let selector = TierSelector::default();
        assert_eq!(selector.pick(1_000_000, 0), Resolution::Raw);
    }
}
