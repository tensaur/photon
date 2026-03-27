use clickhouse::Row;
use serde::{Deserialize, Serialize};

use photon_core::domain::run::{Run, RunStatus};
use photon_core::types::id::{ExperimentId, ProjectId, RunId, UserId};

use crate::ports::{ReadError, ReadRepository, WriteError, WriteRepository};

#[derive(Row, Serialize, Deserialize)]
struct RunRow {
    #[serde(with = "clickhouse::serde::uuid")]
    id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    project_id: uuid::Uuid,
    #[serde(with = "clickhouse::serde::uuid")]
    experiment_id: uuid::Uuid,
    has_experiment: bool,
    #[serde(with = "clickhouse::serde::uuid")]
    user_id: uuid::Uuid,
    name: String,
    status: String,
    status_reason: String,
    tags: Vec<String>,
    created_at: i64,
    updated_at: i64,
}

impl From<&Run> for RunRow {
    fn from(r: &Run) -> Self {
        let (status, status_reason) = match r.status() {
            RunStatus::Created => ("created".to_string(), String::new()),
            RunStatus::Running => ("running".to_string(), String::new()),
            RunStatus::Finished => ("finished".to_string(), String::new()),
            RunStatus::Failed { reason } => ("failed".to_string(), reason.clone()),
        };

        Self {
            id: r.id().into(),
            project_id: r.project_id().into(),
            experiment_id: r.experiment_id().map_or(uuid::Uuid::nil(), Into::into),
            has_experiment: r.experiment_id().is_some(),
            user_id: r.user_id().into(),
            name: r.name().to_owned(),
            status,
            status_reason,
            tags: r.tags().to_vec(),
            created_at: r.created_at().timestamp_millis(),
            updated_at: r.updated_at().timestamp_millis(),
        }
    }
}

impl From<RunRow> for Run {
    fn from(r: RunRow) -> Self {
        let status = match r.status.as_str() {
            "running" => RunStatus::Running,
            "finished" => RunStatus::Finished,
            "failed" => RunStatus::Failed {
                reason: r.status_reason,
            },
            _ => RunStatus::Created,
        };

        Run::restore(
            RunId::from(r.id),
            ProjectId::from(r.project_id),
            if r.has_experiment {
                Some(ExperimentId::from(r.experiment_id))
            } else {
                None
            },
            UserId::from(r.user_id),
            r.name,
            status,
            r.tags,
            chrono::DateTime::from_timestamp_millis(r.created_at)
                .unwrap_or_default(),
            chrono::DateTime::from_timestamp_millis(r.updated_at)
                .unwrap_or_default(),
        )
    }
}

#[derive(Clone)]
pub struct ClickHouseRunStore {
    client: clickhouse::Client,
}

impl ClickHouseRunStore {
    pub fn new(client: clickhouse::Client) -> Self {
        Self { client }
    }
}

impl WriteRepository<Run> for ClickHouseRunStore {
    async fn upsert(&self, run: &Run) -> Result<(), WriteError> {
        let mut insert = self
            .client
            .insert("runs")
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        insert
            .write(&RunRow::from(run))
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;

        insert
            .end()
            .await
            .map_err(|e| WriteError::Store(Box::new(e)))?;
        Ok(())
    }
}

impl ReadRepository<Run> for ClickHouseRunStore {
    async fn list(&self) -> Result<Vec<Run>, ReadError> {
        let rows: Vec<RunRow> = self
            .client
            .query("SELECT ?fields FROM runs FINAL")
            .fetch_all()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(rows.into_iter().map(Run::from).collect())
    }

    async fn get(&self, id: &RunId) -> Result<Option<Run>, ReadError> {
        let uuid: uuid::Uuid = (*id).into();

        let rows: Vec<RunRow> = self
            .client
            .query("SELECT ?fields FROM runs FINAL WHERE id = ?")
            .bind(uuid)
            .fetch_all()
            .await
            .map_err(|e| ReadError::Store(Box::new(e)))?;

        Ok(rows.into_iter().next().map(Run::from))
    }
}
