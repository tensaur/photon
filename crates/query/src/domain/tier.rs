use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lod {
    Raw,
    Bucketed(usize),
}

impl Lod {
    /// Returns true if `self` is a coarser resolution than `other`.
    /// `Raw` is finest; `Bucketed(0)` is next; higher index = coarser.
    pub fn is_coarser_than(&self, other: &Lod) -> bool {
        match (self, other) {
            (Lod::Raw, _) => false,
            (Lod::Bucketed(_), Lod::Raw) => true,
            (Lod::Bucketed(a), Lod::Bucketed(b)) => a > b,
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

    /// Pick the tier whose bucket count is closest to `target_points` from
    /// above. If no tier produces at least `target_points` buckets, return
    /// `Lod::Raw`.
    pub fn pick(&self, point_count: usize, target_points: usize) -> Lod {
        if target_points == 0 {
            return Lod::Raw;
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

        best.map_or(Lod::Raw, |(i, _)| Lod::Bucketed(i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_when_few_points() {
        let selector = TierSelector::new(vec![100, 1000]);
        // 50 points: no tier has >= 200 buckets
        assert_eq!(selector.pick(50, 200), Lod::Raw);
    }

    #[test]
    fn raw_when_no_tier_meets_budget() {
        let selector = TierSelector::new(vec![100, 1000]);
        // 5000 points, budget 200: width 100 → 50 < 200, width 1000 → 5 < 200
        assert_eq!(selector.pick(5_000, 200), Lod::Raw);
    }

    #[test]
    fn picks_closest_fit_above_budget() {
        let selector = TierSelector::new(vec![100, 1000]);
        // 200k points, budget 500:
        //   width 100  → 2000 >= 500
        //   width 1000 →  200 < 500
        // Only tier 0 qualifies
        assert_eq!(selector.pick(200_000, 500), Lod::Bucketed(0));
    }

    #[test]
    fn prefers_tighter_fit() {
        let selector = TierSelector::new(vec![100, 1000]);
        // 200k points, budget 150:
        //   width 100  → 2000 >= 150
        //   width 1000 →  200 >= 150
        // Tier 1 (200) is closer to 150 than tier 0 (2000)
        assert_eq!(selector.pick(200_000, 150), Lod::Bucketed(1));
    }

    #[test]
    fn falls_back_to_finer_tier() {
        let selector = TierSelector::default(); // [100, 1000, 10000]
        // 20k points, budget 100:
        //   tier 0 → 200 >= 100
        //   tier 1 →  20 < 100
        //   tier 2 →   2 < 100
        assert_eq!(selector.pick(20_000, 100), Lod::Bucketed(0));
    }

    #[test]
    fn coarsest_tier_for_large_dataset() {
        let selector = TierSelector::default(); // [100, 1000, 10000]
        // 10M points, budget 500:
        //   tier 0 → 100_000
        //   tier 1 →  10_000
        //   tier 2 →   1_000
        // Tier 2 (1000) is closest to 500 from above
        assert_eq!(selector.pick(10_000_000, 500), Lod::Bucketed(2));
    }

    #[test]
    fn raw_when_zero_budget() {
        let selector = TierSelector::default();
        assert_eq!(selector.pick(1_000_000, 0), Lod::Raw);
    }
}
