use std::future::Future;

use photon_core::domain::experiment::Experiment;
use photon_core::types::id::ExperimentId;

use super::{ReadError, WriteError};

/// Read access to experiment metadata.
pub trait ExperimentReader: Send + Sync + Clone + 'static {
    fn list_experiments(&self) -> impl Future<Output = Result<Vec<Experiment>, ReadError>> + Send;

    fn get_experiment(
        &self,
        id: &ExperimentId,
    ) -> impl Future<Output = Result<Option<Experiment>, ReadError>> + Send;
}

/// Write access to experiment metadata.
pub trait ExperimentWriter: Send + Sync + Clone + 'static {
    fn upsert_experiment(
        &self,
        experiment: &Experiment,
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}
