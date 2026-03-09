use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;

use super::{ReadError, WriteError};

/// Tracks how far each tier has been compacted for a given run and metric.
/// The offset is a raw point count, not a step value.
pub trait CompactionCursor: Send + Sync + Clone + 'static {
    fn get(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
    ) -> impl Future<Output = Result<Option<u64>, ReadError>> + Send;

    fn advance(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        offset: u64,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}
