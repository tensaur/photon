pub mod experiment;
pub mod project;
pub mod run;
pub mod tenant;

use crate::types::id::{ExperimentId, ProjectId, RunId};

/// Marker trait for domain entities with a typed identifier.
pub trait Entity: Send + 'static {
    type Id: Send + Sync;
}

impl Entity for run::Run {
    type Id = RunId;
}

impl Entity for project::Project {
    type Id = ProjectId;
}

impl Entity for experiment::Experiment {
    type Id = ExperimentId;
}
