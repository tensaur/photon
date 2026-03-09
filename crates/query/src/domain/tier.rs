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
pub struct TierSelector {
    divisors: Vec<usize>,
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
            .map(|(i, _)| Resolution::Bucketed(i))
            .unwrap_or(Resolution::Raw);

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
