use photon_core::types::metric::MetricKey;

#[derive(Clone, Copy, Debug)]
pub struct RawPoint {
    pub key: MetricKey,
    pub value: f64,
    pub step: u64,
    pub timestamp_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct BatchStats {
    pub batches_created: u64,
    pub points_batched: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
}
