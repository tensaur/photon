/// Thins a point sequence to a target count.
pub trait Selector: Send + Sync + Clone + 'static {
    fn select(&self, points: &[(u64, f64)], target: usize) -> Vec<(u64, f64)>;
}
