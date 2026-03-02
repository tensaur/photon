use crate::types::metric::MetricPoint;

pub trait PointResolver: Send + 'static {
    type Point: Copy + Send + 'static;

    fn resolve(&self, points: &[Self::Point]) -> Vec<MetricPoint>;
}
