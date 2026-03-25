use std::error::Error;

use photon_core::types::id::RunId;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ListRunsError {
    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ListMetricsError {
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: RunId },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum QueryMetricsError {
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: RunId },

    #[error("metric '{metric}' not found for run {run_id}")]
    MetricNotFound { run_id: RunId, metric: String },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: RunId },

    #[error("already subscribed to run {run_id}")]
    AlreadySubscribed { run_id: RunId },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum UnsubscribeError {
    #[error("not subscribed to run {run_id}")]
    NotSubscribed { run_id: RunId },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum UpdateStatusError {
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: RunId },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum AddTagError {
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: RunId },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum DeleteRunError {
    #[error("run not found: {run_id}")]
    RunNotFound { run_id: RunId },

    #[error("unknown error: {0}")]
    Unknown(#[source] Box<dyn Error + Send + Sync>),
}
