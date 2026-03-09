use std::ops::Range;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    Raw,
    Bucketed(u64),
}

#[derive(Clone, Debug)]
pub struct ResolutionPlan {
    pub line: Resolution,
    pub envelope: Resolution,
}

/// Picks the right resolution for the line and envelope based on
/// the step range and target point count.
pub struct TierSelector {
    tier_widths: Vec<u64>,
}

impl TierSelector {
    pub fn new(tier_widths: Vec<u64>) -> Self {
        debug_assert!(
            tier_widths.windows(2).all(|w| w[0] < w[1]),
            "tier widths must be sorted ascending"
        );

        Self { tier_widths }
    }

    pub fn pick(&self, step_range: &Range<u64>, target: usize) -> ResolutionPlan {
        let span = step_range.end.saturating_sub(step_range.start);
        let target = target as u64;

        let line = self
            .tier_widths
            .iter()
            .find(|&&w| w > 0 && span / w >= target)
            .map(|&w| Resolution::Bucketed(w))
            .unwrap_or(Resolution::Raw);

        let envelope = match line {
            Resolution::Raw => Resolution::Raw,
            Resolution::Bucketed(line_width) => self
                .tier_widths
                .iter()
                .rev()
                .find(|&&w| w > 0 && span / w >= target)
                .map(|&w| Resolution::Bucketed(w))
                .unwrap_or(Resolution::Bucketed(line_width)),
        };

        ResolutionPlan { line, envelope }
    }
}
