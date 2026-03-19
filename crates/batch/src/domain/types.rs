#[derive(Clone, Debug, Default)]
pub struct BatchStats {
    pub batches_created: u64,
    pub points_batched: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
}
