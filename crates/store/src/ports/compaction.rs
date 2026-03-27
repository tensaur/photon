use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};

use super::{ReadError, WriteError};

/// Tracks how far each tier has been compacted for a given run and metric.
/// The step value is the exclusive upper bound of compacted data — all steps
/// below this value have been aggregated into buckets.
pub trait CompactionCursor: Send + Sync + Clone + 'static {
    fn get(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
    ) -> impl Future<Output = Result<Option<Step>, ReadError>> + Send;

    fn advance(
        &self,
        run_id: &RunId,
        key: &Metric,
        tier: usize,
        through: Step,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}
