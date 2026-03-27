use photon_core::types::metric::Step;

use crate::ports::selector::Selector;

#[derive(Clone)]
pub struct NoOpSelector;

impl Selector for NoOpSelector {
    fn select(&self, points: &[(Step, f64)], _target: usize) -> Vec<(Step, f64)> {
        points.to_vec()
    }
}
