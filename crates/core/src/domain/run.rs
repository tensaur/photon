use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::id::{ExperimentId, ProjectId, RunId, UserId};

/// Intent to create a new run. Separates user-provided fields from
/// system-generated fields (id, timestamps, status).
pub struct CreateRunRequest {
    pub name: String,
    pub project_id: ProjectId,
    pub experiment_id: Option<ExperimentId>,
    pub user_id: UserId,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    id: RunId,
    project_id: ProjectId,
    experiment_id: Option<ExperimentId>,
    user_id: UserId,
    name: String,
    status: RunStatus,
    tags: Vec<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Run {
    /// Create a new run from a request. Generates id and timestamps.
    pub fn create(req: CreateRunRequest) -> Self {
        let now = Utc::now();
        Self {
            id: RunId::new(),
            project_id: req.project_id,
            experiment_id: req.experiment_id,
            user_id: req.user_id,
            name: req.name,
            status: RunStatus::Created,
            tags: req.tags,
            created_at: now,
            updated_at: now,
        }
    }

    /// Reconstruct a run from persisted data. No validation is performed.
    pub fn restore(
        id: RunId,
        project_id: ProjectId,
        experiment_id: Option<ExperimentId>,
        user_id: UserId,
        name: String,
        status: RunStatus,
        tags: Vec<String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            project_id,
            experiment_id,
            user_id,
            name,
            status,
            tags,
            created_at,
            updated_at,
        }
    }

    // --- Getters ---

    pub fn id(&self) -> RunId {
        self.id
    }

    pub fn project_id(&self) -> ProjectId {
        self.project_id
    }

    pub fn experiment_id(&self) -> Option<ExperimentId> {
        self.experiment_id
    }

    pub fn user_id(&self) -> UserId {
        self.user_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn status(&self) -> &RunStatus {
        &self.status
    }

    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    // --- Transitions ---

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::id::{ProjectId, UserId};

    fn make_run(status: RunStatus) -> Run {
        let mut run = Run::create(CreateRunRequest {
            name: "test-run".to_string(),
            project_id: ProjectId::new(),
            experiment_id: None,
            user_id: UserId::new(),
            tags: vec![],
        });
        run.status = status;
        run
    }

    #[test]
    fn test_start_from_created() {
        let mut run = make_run(RunStatus::Created);
        assert!(run.start().is_ok());
        assert_eq!(*run.status(), RunStatus::Running);
    }

    #[test]
    fn test_start_from_running_fails() {
        let mut run = make_run(RunStatus::Running);
        assert!(run.start().is_err());
    }

    #[test]
    fn test_finish_from_running() {
        let mut run = make_run(RunStatus::Running);
        assert!(run.finish().is_ok());
        assert_eq!(*run.status(), RunStatus::Finished);
    }

    #[test]
    fn test_finish_from_created_fails() {
        let mut run = make_run(RunStatus::Created);
        assert!(run.finish().is_err());
    }

    #[test]
    fn test_fail_from_created() {
        let mut run = make_run(RunStatus::Created);
        assert!(run.fail("boom".to_string()).is_ok());
        assert_eq!(
            *run.status(),
            RunStatus::Failed {
                reason: "boom".to_string()
            }
        );
    }

    #[test]
    fn test_fail_from_running() {
        let mut run = make_run(RunStatus::Running);
        assert!(run.fail("timeout".to_string()).is_ok());
        assert_eq!(
            *run.status(),
            RunStatus::Failed {
                reason: "timeout".to_string()
            }
        );
    }

    #[test]
    fn test_fail_from_finished_fails() {
        let mut run = make_run(RunStatus::Finished);
        assert!(run.fail("late error".to_string()).is_err());
    }

    #[test]
    fn test_is_active() {
        assert!(make_run(RunStatus::Created).is_active());
        assert!(make_run(RunStatus::Running).is_active());
        assert!(!make_run(RunStatus::Finished).is_active());
        assert!(!make_run(RunStatus::Failed {
            reason: "err".to_string()
        })
        .is_active());
    }
}
