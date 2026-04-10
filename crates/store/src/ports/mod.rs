pub mod bucket;
pub mod finalised;
pub mod metric;
pub mod repository;
pub mod watermark;

pub use repository::{ReadRepository, WriteRepository};

use photon_core::types::id::RunId;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("invalid batch for run {run_id}: {reason}")]
    InvalidBatch { run_id: RunId, reason: String },

    #[error("store error: {0}")]
    Store(Box<dyn std::error::Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("run {0} not found")]
    RunNotFound(RunId),

    #[error("store error: {0}")]
    Store(Box<dyn std::error::Error + Send + Sync>),
}
