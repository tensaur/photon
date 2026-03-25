use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::id::{ExperimentId, ProjectId, RunId, UserId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub project_id: ProjectId,
    pub experiment_id: Option<ExperimentId>,
    pub user_id: UserId,
    pub name: String,
    pub status: RunStatus,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Run {
    pub fn start(&mut self) -> Result<(), RunStatusError> {
        match self.status {
            RunStatus::Created => {
                self.status = RunStatus::Running;
                Ok(())
            }
            _ => Err(RunStatusError::InvalidTransition {
                from: self.status.clone(),
                to: RunStatus::Running,
            }),
        }
    }

    pub fn finish(&mut self) -> Result<(), RunStatusError> {
        match self.status {
            RunStatus::Running => {
                self.status = RunStatus::Finished;
                Ok(())
            }
            _ => Err(RunStatusError::InvalidTransition {
                from: self.status.clone(),
                to: RunStatus::Finished,
            }),
        }
    }

    pub fn fail(&mut self, reason: String) -> Result<(), RunStatusError> {
        match self.status {
            RunStatus::Created | RunStatus::Running => {
                self.status = RunStatus::Failed { reason };
                Ok(())
            }
            _ => Err(RunStatusError::InvalidTransition {
                from: self.status.clone(),
                to: RunStatus::Failed {
                    reason: String::new(),
                },
            }),
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, RunStatus::Created | RunStatus::Running)
    }
}

/// Status of a run. Transitions are enforced by the methods on `Run`:
///   Created → Running → Finished
///                ↘
///                 Failed
///   Created ──────↗
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Created,
    Running,
    Finished,
    Failed { reason: String },
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum RunStatusError {
    #[error("cannot transition from {from:?} to {to:?}")]
    InvalidTransition { from: RunStatus, to: RunStatus },
}
