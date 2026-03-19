use super::interner::MetricKey;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawPoint {
    pub key: MetricKey,
    pub value: f64,
    pub step: u64,
    pub timestamp_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct FlushStats {
    pub batches_flushed: u64,
    pub points_flushed: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
}
