#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    Raw,
    Bucketed(usize),
}

#[derive(Clone, Debug)]
pub struct ResolutionPlan {
    pub line: Resolution,
    pub envelope: Resolution,
}

/// Picks the right resolution for the line and envelope based on
/// the step range and target point count.
#[derive(Clone)]
pub struct TierSelector {
    divisors: Vec<usize>,
}

impl Default for TierSelector {
    fn default() -> Self {
        Self::new(vec![60, 3600])
    }
}

impl TierSelector {
    pub fn new(divisors: Vec<usize>) -> Self {
        debug_assert!(
            divisors.windows(2).all(|w| w[0] < w[1]),
            "divisors must be sorted ascending"
        );
        Self { divisors }
    }

    pub fn pick(&self, point_count: usize, target: usize) -> ResolutionPlan {
        let line = self
            .divisors
            .iter()
            .enumerate()
            .find(|(_, d)| point_count / **d >= target)
            .map_or(Resolution::Raw, |(i, _)| Resolution::Bucketed(i));

        let envelope = match line {
            Resolution::Raw => Resolution::Raw,
            Resolution::Bucketed(_) => self
                .divisors
                .iter()
                .enumerate()
                .rev()
                .find(|(_, d)| point_count / **d >= target)
                .map(|(i, _)| Resolution::Bucketed(i))
                .unwrap_or(line.clone()),
        };

        ResolutionPlan { line, envelope }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_when_few_points() {
        let selector = TierSelector::new(vec![60, 3600]);
        // 100 points with a target of 200 and no divisor can satisfy
        // point_count / d >= target, so we get Raw.
        let plan = selector.pick(100, 200);
        assert_eq!(plan.line, Resolution::Raw);
        assert_eq!(plan.envelope, Resolution::Raw);
    }

    #[test]
    fn test_bucketed_when_many_points() {
        let selector = TierSelector::new(vec![60, 3600]);
        // 720_000 points, target 200.
        // divisor 60:   720_000 / 60   = 12_000 >= 200  -> Bucketed(0)  (line picks first match)
        // divisor 3600: 720_000 / 3600 =    200 >= 200  -> Bucketed(1)  (envelope picks last match)
        let plan = selector.pick(720_000, 200);
        assert_eq!(plan.line, Resolution::Bucketed(0));
        assert_eq!(plan.envelope, Resolution::Bucketed(1));
    }

    #[test]
    fn test_default_tier_selector() {
        let selector = TierSelector::default();
        // Default divisors are [60, 3600] with 1-minute and 1-hour buckets.
        // A small point count should still resolve to Raw.
        let plan = selector.pick(50, 500);
        assert_eq!(plan.line, Resolution::Raw);
        assert_eq!(plan.envelope, Resolution::Raw);

        // A large point count should resolve to Bucketed.
        let plan = selector.pick(1_000_000, 500);
        assert_eq!(plan.line, Resolution::Bucketed(0));
        assert!(matches!(plan.envelope, Resolution::Bucketed(_)));
    }
}
