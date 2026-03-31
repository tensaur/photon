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

/// Picks the right resolution tier for the line and envelope.
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

    pub fn pick(&self, point_count: usize, target: usize) -> ResolutionPlan {
        let line = self
            .widths
            .iter()
            .enumerate()
            .find(|(_, w)| point_count / **w as usize >= target)
            .map_or(Resolution::Raw, |(i, _)| Resolution::Bucketed(i));

        let envelope = match line {
            Resolution::Raw => Resolution::Raw,
            Resolution::Bucketed(_) => self
                .widths
                .iter()
                .enumerate()
                .rev()
                .find(|(_, w)| point_count / **w as usize >= target)
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
        let selector = TierSelector::new(vec![100, 1000]);
        let plan = selector.pick(50, 200);
        assert_eq!(plan.line, Resolution::Raw);
        assert_eq!(plan.envelope, Resolution::Raw);
    }

    #[test]
    fn test_bucketed_when_many_points() {
        let selector = TierSelector::new(vec![100, 1000]);
        // 200_000 points, target 200.
        // width 100:  200_000 / 100  = 2_000 >= 200 -> Bucketed(0)
        // width 1000: 200_000 / 1000 =   200 >= 200 -> Bucketed(1)
        let plan = selector.pick(200_000, 200);
        assert_eq!(plan.line, Resolution::Bucketed(0));
        assert_eq!(plan.envelope, Resolution::Bucketed(1));
    }

    #[test]
    fn test_default_tier_selector() {
        let selector = TierSelector::default();
        let plan = selector.pick(50, 500);
        assert_eq!(plan.line, Resolution::Raw);
        assert_eq!(plan.envelope, Resolution::Raw);

        let plan = selector.pick(1_000_000, 500);
        assert_eq!(plan.line, Resolution::Bucketed(0));
        assert!(matches!(plan.envelope, Resolution::Bucketed(_)));
    }
}
