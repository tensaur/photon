use std::future::Future;

use photon_core::types::id::RunId;

use super::{ReadError, WriteError};

/// Tracks whether a run has been fully persisted and indexed.
pub trait FinalisedStore: Send + Sync + Clone + 'static {
    fn mark_finalised(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;

    fn mark_finalised_many(
        &self,
        run_ids: &[RunId],
    ) -> impl Future<Output = Result<(), WriteError>> + Send;

    fn is_finalised(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<bool, ReadError>> + Send;
}
