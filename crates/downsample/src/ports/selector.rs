use photon_core::types::metric::Step;

/// Thins a point sequence to a target count.
pub trait Selector: Send + Sync + Clone + 'static {
    fn select(&self, points: &[(Step, f64)], target: usize) -> Vec<(Step, f64)>;
}
