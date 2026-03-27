use serde::{Deserialize, Serialize};

use crate::domain::experiment::Experiment;
use crate::domain::project::Project;
use crate::domain::run::Run;
use crate::types::ack::AckResult;
use crate::types::batch::WireBatch;
use crate::types::error::ApiError;
use crate::types::id::{ExperimentId, ProjectId, RunId};
use crate::types::sequence::SequenceNumber;

/// Envelope for ingest requests over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IngestMessage {
    Batch(WireBatch),
    RegisterRun(Run),
    RegisterExperiment(Experiment),
    RegisterProject(Project),
    QueryWatermark(RunId),
}

/// Envelope for ingest responses over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IngestResult {
    Ack(AckResult),
    RunRegistered(RunId),
    ExperimentRegistered(ExperimentId),
    ProjectRegistered(ProjectId),
    Watermark(SequenceNumber),
    Error(ApiError),
}
