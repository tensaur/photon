use crate::types::metric::Metric;

#[derive(Clone, Debug, PartialEq)]
pub struct Bucket {
    pub step_start: u64,
    pub step_end: u64,
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
