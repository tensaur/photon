#[derive(Clone, Debug, PartialEq)]
pub struct Bucket {
    pub step: u64,
    pub value: f64,
    pub min: f64,
    pub max: f64,
}
