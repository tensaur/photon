use crate::types::metric::{Metric, Step};

#[derive(Clone, Debug, PartialEq)]
pub struct Bucket {
    pub step_start: Step,
    pub step_end: Step,
    pub value: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Clone, Debug)]
pub struct BucketEntry {
    pub key: Metric,
    pub tier: usize,
    pub bucket: Bucket,
}
