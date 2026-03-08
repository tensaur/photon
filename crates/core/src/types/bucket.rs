use crate::types::metric::Metric;

#[derive(Clone, Debug, PartialEq)]
pub struct Bucket {
    pub step: u64,
    pub value: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Clone, Debug)]
pub struct BucketEntry {
    pub key: Metric,
    pub tier: u64,
    pub bucket: Bucket,
}

