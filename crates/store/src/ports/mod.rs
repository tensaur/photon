pub mod bucket;
pub mod metric;
pub mod watermark;
pub mod compaction;

use photon_core::types::id::RunId;

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("invalid batch for run {run_id}: {reason}")]
    InvalidBatch { run_id: RunId, reason: String },

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("run {0} not found")]
    RunNotFound(RunId),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}
