use crate::ports::selector::Selector;

#[derive(Clone)]
pub struct NoOpSelector;

impl Selector for NoOpSelector {
    fn select(&self, points: &[(u64, f64)], _target: usize) -> Vec<(u64, f64)> {
        points.to_vec()
    }
}
