use std::future::Future;

use photon_core::domain::run::RunStatus;
use photon_core::types::id::RunId;

use crate::domain::error::{AddTagError, DeleteRunError, UpdateStatusError};

#[cfg(not(target_arch = "wasm32"))]
pub trait RunCommander: Clone + Send + Sync + 'static {
    fn update_status(
        &self,
        run_id: &RunId,
        status: RunStatus,
    ) -> impl Future<Output = Result<(), UpdateStatusError>> + Send;

    fn add_tag(
        &self,
        run_id: &RunId,
        tag: String,
    ) -> impl Future<Output = Result<(), AddTagError>> + Send;

    fn delete_run(&self, run_id: &RunId)
    -> impl Future<Output = Result<(), DeleteRunError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait RunCommander: Clone + 'static {
    fn update_status(
        &self,
        run_id: &RunId,
        status: RunStatus,
    ) -> impl Future<Output = Result<(), UpdateStatusError>>;

    fn add_tag(&self, run_id: &RunId, tag: String)
    -> impl Future<Output = Result<(), AddTagError>>;

    fn delete_run(&self, run_id: &RunId) -> impl Future<Output = Result<(), DeleteRunError>>;
}
