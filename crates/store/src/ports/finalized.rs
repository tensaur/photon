use std::future::Future;

use photon_core::types::id::RunId;

use super::{ReadError, WriteError};

/// Tracks whether a run has been fully persisted and indexed — i.e. all
/// derived state (downsampled buckets, etc.) has been materialized and the
/// run is safe to query at full fidelity.
pub trait FinalizedStore: Send + Sync + Clone + 'static {
    fn mark_finalized(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;

    fn is_finalized(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<bool, ReadError>> + Send;
}
