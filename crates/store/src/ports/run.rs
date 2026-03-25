use std::future::Future;

use photon_core::domain::run::Run;
use photon_core::types::id::RunId;

use super::{ReadError, WriteError};

/// Read access to run metadata.
pub trait RunReader: Send + Sync + Clone + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<Run>, ReadError>> + Send;

    fn get_run(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Option<Run>, ReadError>> + Send;
}

/// Write access to run metadata.
pub trait RunWriter: Send + Sync + Clone + 'static {
    fn upsert_run(&self, run: &Run) -> impl Future<Output = Result<(), WriteError>> + Send;
}
